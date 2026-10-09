//! One-attempt, deadline-bounded native observation orchestration.

use core::future::Future;
use core::pin::Pin;
use std::time::Instant;

use crate::core::{
    BoundGtidSet, CollectionFailure, NativeObservationBracket, NativeObservationDraft,
    NativeSnapshot, ObservationInstant, ObservationMetadata, ObservationOutcome, StaleReason,
};

use crate::adapter::decode::{
    assemble_snapshot, decode_executed_gtids, decode_local_state, decode_members, decode_product,
    decode_view, validate_server_identity,
};
use crate::adapter::error::{ErrorDisposition, MappedError, map_decode, map_session, map_socket};
use crate::adapter::query::QueryId;
use crate::adapter::session::{MysqlNativeSession, NativeSession, SessionError};
use crate::adapter::{
    AdapterDiagnostic, DeadlineStage, ObservationClock, ObservationReport, ObservationRequest,
    SocketPathError, UnixSocketPath,
};

/// Stateless observer for one direct local MySQL attempt.
#[derive(Clone, Copy, Debug, Default)]
pub struct MysqlObserver;

impl MysqlObserver {
    /// Observes exactly one caller-selected Unix-domain socket once.
    pub async fn observe<C: ObservationClock>(request: ObservationRequest<C>) -> ObservationReport {
        observe_with(&request, &MysqlSessionConnector).await
    }
}

pub(crate) type SessionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn NativeSession + Send>, SessionError>> + Send + 'a>>;

pub(crate) trait SessionConnector<C: ObservationClock> {
    fn revalidate(&self, socket: &UnixSocketPath) -> Result<(), SocketPathError>;

    fn connect<'a>(&'a self, request: &'a ObservationRequest<C>) -> SessionFuture<'a>;
}

struct MysqlSessionConnector;

impl<C: ObservationClock> SessionConnector<C> for MysqlSessionConnector {
    fn revalidate(&self, socket: &UnixSocketPath) -> Result<(), SocketPathError> {
        socket.revalidate()
    }

    fn connect<'a>(&'a self, request: &'a ObservationRequest<C>) -> SessionFuture<'a> {
        Box::pin(async move {
            MysqlNativeSession::connect(request)
                .await
                .map(|session| Box::new(session) as Box<dyn NativeSession + Send>)
        })
    }
}

#[derive(Default)]
struct Collected {
    opening: Option<NativeSnapshot>,
    executed: Option<crate::core::GtidSet>,
    closing: Option<NativeSnapshot>,
}

enum Terminal {
    Failure(MappedError),
    Timeout(DeadlineStage),
}

pub(crate) async fn observe_with<C, S>(
    request: &ObservationRequest<C>,
    connector: &S,
) -> ObservationReport
where
    C: ObservationClock,
    S: SessionConnector<C>,
{
    let start = request.clock().clock().now();
    let deadline = request.clock().deadline();
    let runtime_deadline = match request.clock().runtime_deadline() {
        Ok(deadline) => deadline,
        Err(_) => {
            return finish_failure(
                request,
                start,
                Collected::default(),
                MappedError {
                    disposition: ErrorDisposition::Collection(CollectionFailure::Malformed(
                        crate::core::MalformedReason::TimingOrder,
                    )),
                    diagnostic: AdapterDiagnostic::Client {
                        stage: crate::adapter::ObservationStage::SocketValidation,
                        surface: None,
                    },
                },
            );
        }
    };

    if is_expired(request, deadline) {
        return finish_timeout(
            request,
            start,
            Collected::default(),
            DeadlineStage::SocketValidation,
        );
    }
    let socket_validation = connector.revalidate(request.socket());
    if is_expired(request, deadline) {
        return finish_timeout(
            request,
            start,
            Collected::default(),
            DeadlineStage::SocketValidation,
        );
    }
    if let Err(error) = socket_validation {
        return finish_failure(request, start, Collected::default(), map_socket(error));
    }

    let mut session = match run_until(runtime_deadline, connector.connect(request)).await {
        Ok(Ok(session)) => session,
        Ok(Err(_)) if is_expired(request, deadline) => {
            return finish_timeout(request, start, Collected::default(), DeadlineStage::Connect);
        }
        Ok(Err(error)) => {
            return finish_failure(request, start, Collected::default(), map_session(&error));
        }
        Err(()) => {
            return finish_timeout(request, start, Collected::default(), DeadlineStage::Connect);
        }
    };
    if is_expired(request, deadline) {
        let _ = run_until(runtime_deadline, session.disconnect()).await;
        return finish_timeout(request, start, Collected::default(), DeadlineStage::Connect);
    }

    let mut collected = Collected::default();
    let terminal = collect(
        request,
        session.as_mut(),
        runtime_deadline,
        deadline,
        &mut collected,
    )
    .await;
    let collection_end = request.clock().clock().now();

    let (disconnect_error, disconnect_timed_out) =
        match run_until(runtime_deadline, session.disconnect()).await {
            Ok(Ok(())) => (None, false),
            Ok(Err(error)) => (Some(map_session(&error)), false),
            Err(()) => (None, true),
        };
    let decision = request.clock().clock().now();

    if collection_end > deadline || decision > deadline || disconnect_timed_out {
        let timeout_stage = match terminal.as_ref() {
            Some(Terminal::Timeout(stage)) => *stage,
            _ if disconnect_error.is_some() || disconnect_timed_out => DeadlineStage::Disconnect,
            _ => DeadlineStage::Completion,
        };
        return report_timeout(request, start, collection_end, decision, timeout_stage);
    }

    match terminal {
        Some(Terminal::Timeout(stage)) => {
            report_timeout(request, start, collection_end, decision, stage)
        }
        Some(Terminal::Failure(error)) => {
            report_failure(request, start, collection_end, decision, collected, error)
        }
        None => match disconnect_error {
            Some(error) => {
                report_failure(request, start, collection_end, decision, collected, error)
            }
            None => report_success(request, start, collection_end, decision, collected),
        },
    }
}

async fn collect<C: ObservationClock>(
    request: &ObservationRequest<C>,
    session: &mut (dyn NativeSession + Send),
    runtime_deadline: Instant,
    deadline: ObservationInstant,
    collected: &mut Collected,
) -> Option<Terminal> {
    let product = match query(
        request,
        session,
        QueryId::Mysql8411ProductIdentityV1,
        runtime_deadline,
        deadline,
        DeadlineStage::ProductIdentity,
    )
    .await
    {
        Ok(result) => {
            let decoded = decode_product(&result);
            if is_expired(request, deadline) {
                return Some(Terminal::Timeout(DeadlineStage::ProductIdentity));
            }
            match decoded.map_err(map_decode) {
                Ok(product) => product,
                Err(error) => return Some(Terminal::Failure(error)),
            }
        }
        Err(terminal) => return Some(terminal),
    };
    let identity = validate_server_identity(request.binding(), &product);
    if is_expired(request, deadline) {
        return Some(Terminal::Timeout(DeadlineStage::ProductIdentity));
    }
    if let Err(error) = identity.map_err(map_decode) {
        return Some(Terminal::Failure(error));
    }

    match collect_snapshot(request, session, runtime_deadline, deadline, true).await {
        Ok(snapshot) => collected.opening = Some(snapshot),
        Err(terminal) => return Some(terminal),
    }

    let result = match query(
        request,
        session,
        QueryId::Mysql8411ExecutedGtidsV1,
        runtime_deadline,
        deadline,
        DeadlineStage::ExecutedGtids,
    )
    .await
    {
        Ok(result) => result,
        Err(terminal) => return Some(terminal),
    };
    let decoded = decode_executed_gtids(&result);
    if is_expired(request, deadline) {
        return Some(Terminal::Timeout(DeadlineStage::ExecutedGtids));
    }
    match decoded.map_err(map_decode) {
        Ok(executed) => collected.executed = Some(executed),
        Err(error) => return Some(Terminal::Failure(error)),
    }

    match collect_snapshot(request, session, runtime_deadline, deadline, false).await {
        Ok(snapshot) => collected.closing = Some(snapshot),
        Err(terminal) => return Some(terminal),
    }
    None
}

async fn collect_snapshot<C: ObservationClock>(
    request: &ObservationRequest<C>,
    session: &mut (dyn NativeSession + Send),
    runtime_deadline: Instant,
    deadline: ObservationInstant,
    opening: bool,
) -> Result<NativeSnapshot, Terminal> {
    let local_stage = if opening {
        DeadlineStage::OpeningLocalState
    } else {
        DeadlineStage::ClosingLocalState
    };
    let members_stage = if opening {
        DeadlineStage::OpeningMembers
    } else {
        DeadlineStage::ClosingMembers
    };
    let view_stage = if opening {
        DeadlineStage::OpeningView
    } else {
        DeadlineStage::ClosingView
    };
    let local_result = query(
        request,
        session,
        QueryId::Mysql8411LocalStateV1,
        runtime_deadline,
        deadline,
        local_stage,
    )
    .await?;
    let local = decode_local_state(&local_result);
    if is_expired(request, deadline) {
        return Err(Terminal::Timeout(local_stage));
    }
    let local = local.map_err(map_decode).map_err(Terminal::Failure)?;
    let members_result = query(
        request,
        session,
        QueryId::Mysql8411GroupMembersV1,
        runtime_deadline,
        deadline,
        members_stage,
    )
    .await?;
    let members = decode_members(&members_result);
    if is_expired(request, deadline) {
        return Err(Terminal::Timeout(members_stage));
    }
    let members = members.map_err(map_decode).map_err(Terminal::Failure)?;
    let view_result = query(
        request,
        session,
        QueryId::Mysql8411LocalMemberStatsV1,
        runtime_deadline,
        deadline,
        view_stage,
    )
    .await?;
    let view = decode_view(&view_result);
    if is_expired(request, deadline) {
        return Err(Terminal::Timeout(view_stage));
    }
    let view = view.map_err(map_decode).map_err(Terminal::Failure)?;
    let snapshot = assemble_snapshot(request.binding(), local, members, view);
    if is_expired(request, deadline) {
        return Err(Terminal::Timeout(view_stage));
    }
    snapshot.map_err(map_decode).map_err(Terminal::Failure)
}

async fn query<C: ObservationClock>(
    request: &ObservationRequest<C>,
    session: &mut (dyn NativeSession + Send),
    id: QueryId,
    runtime_deadline: Instant,
    deadline: ObservationInstant,
    stage: DeadlineStage,
) -> Result<crate::adapter::query::RawResult, Terminal> {
    if is_expired(request, deadline) {
        return Err(Terminal::Timeout(stage));
    }
    match run_until(runtime_deadline, session.query(id)).await {
        Ok(Ok(result)) if !is_expired(request, deadline) => Ok(result),
        Ok(Ok(_)) | Err(()) => Err(Terminal::Timeout(stage)),
        Ok(Err(_)) if is_expired(request, deadline) => Err(Terminal::Timeout(stage)),
        Ok(Err(error)) => Err(Terminal::Failure(map_session(&error))),
    }
}

async fn run_until<F, T>(deadline: Instant, future: F) -> Result<T, ()>
where
    F: Future<Output = T>,
{
    tokio::time::timeout_at(deadline.into(), future)
        .await
        .map_err(|_| ())
}

fn is_expired<C: ObservationClock>(
    request: &ObservationRequest<C>,
    deadline: ObservationInstant,
) -> bool {
    request.clock().clock().now() > deadline
}

fn report_success<C: ObservationClock>(
    request: &ObservationRequest<C>,
    start: ObservationInstant,
    end: ObservationInstant,
    decision: ObservationInstant,
    collected: Collected,
) -> ObservationReport {
    let metadata = metadata(request, start, end, decision);
    let mut draft = NativeObservationDraft::new(metadata);
    if let Some(opening) = collected.opening {
        draft = draft.opening(NativeObservationBracket::new(
            request.binding().clone(),
            opening,
        ));
    }
    if let Some(executed) = collected.executed {
        draft = draft.executed(BoundGtidSet::new(request.binding().clone(), executed));
    }
    if let Some(closing) = collected.closing {
        draft = draft.closing(NativeObservationBracket::new(
            request.binding().clone(),
            closing,
        ));
    }
    let outcome = draft.finalize();
    let diagnostic = match &outcome {
        ObservationOutcome::Valid(_) => AdapterDiagnostic::None,
        ObservationOutcome::Malformed { reason, .. } => {
            AdapterDiagnostic::Outcome(crate::adapter::CoreOutcomeClass::Malformed(*reason))
        }
        ObservationOutcome::Partial { .. } => {
            AdapterDiagnostic::Outcome(crate::adapter::CoreOutcomeClass::Partial)
        }
        ObservationOutcome::Stale { reason, .. } => {
            AdapterDiagnostic::Outcome(crate::adapter::CoreOutcomeClass::Stale(*reason))
        }
        ObservationOutcome::FutureDated(_) => {
            AdapterDiagnostic::Outcome(crate::adapter::CoreOutcomeClass::FutureDated)
        }
        ObservationOutcome::Incoherent { reason, .. } => {
            AdapterDiagnostic::Outcome(crate::adapter::CoreOutcomeClass::Incoherent(*reason))
        }
        ObservationOutcome::Absent(_)
        | ObservationOutcome::Unreachable(_)
        | ObservationOutcome::PermissionDenied(_)
        | ObservationOutcome::AuthenticationFailure(_)
        | ObservationOutcome::Unsupported { .. } => {
            AdapterDiagnostic::Outcome(crate::adapter::CoreOutcomeClass::Unsupported(
                crate::core::UnsupportedReason::CollectorCapability,
            ))
        }
    };
    ObservationReport::new(outcome, diagnostic)
}

fn report_failure<C: ObservationClock>(
    request: &ObservationRequest<C>,
    start: ObservationInstant,
    end: ObservationInstant,
    decision: ObservationInstant,
    collected: Collected,
    error: MappedError,
) -> ObservationReport {
    let metadata = metadata(request, start, end, decision);
    let outcome = match error.disposition {
        ErrorDisposition::Incoherent(reason) => ObservationOutcome::Incoherent { metadata, reason },
        ErrorDisposition::Collection(failure) => {
            let mut draft = NativeObservationDraft::new(metadata);
            if let Some(opening) = collected.opening {
                draft = draft.opening(NativeObservationBracket::new(
                    request.binding().clone(),
                    opening,
                ));
            }
            if let Some(executed) = collected.executed {
                draft = draft.executed(BoundGtidSet::new(request.binding().clone(), executed));
            }
            if let Some(closing) = collected.closing {
                draft = draft.closing(NativeObservationBracket::new(
                    request.binding().clone(),
                    closing,
                ));
            }
            draft.failure(failure).finalize()
        }
    };
    ObservationReport::new(outcome, error.diagnostic)
}

fn finish_failure<C: ObservationClock>(
    request: &ObservationRequest<C>,
    start: ObservationInstant,
    collected: Collected,
    error: MappedError,
) -> ObservationReport {
    let now = request.clock().clock().now();
    report_failure(request, start, now, now, collected, error)
}

fn finish_timeout<C: ObservationClock>(
    request: &ObservationRequest<C>,
    start: ObservationInstant,
    _collected: Collected,
    stage: DeadlineStage,
) -> ObservationReport {
    let now = request.clock().clock().now();
    report_timeout(request, start, now, now, stage)
}

fn report_timeout<C: ObservationClock>(
    request: &ObservationRequest<C>,
    start: ObservationInstant,
    end: ObservationInstant,
    decision: ObservationInstant,
    stage: DeadlineStage,
) -> ObservationReport {
    ObservationReport::new(
        ObservationOutcome::Stale {
            metadata: metadata(request, start, end, decision),
            reason: StaleReason::Expired,
        },
        AdapterDiagnostic::Timeout { stage },
    )
}

fn metadata<C: ObservationClock>(
    request: &ObservationRequest<C>,
    start: ObservationInstant,
    end: ObservationInstant,
    decision: ObservationInstant,
) -> ObservationMetadata {
    ObservationMetadata::new(
        request.binding().clone(),
        request.provenance().clone(),
        start,
        end,
        request.clock().deadline(),
        decision,
    )
}
