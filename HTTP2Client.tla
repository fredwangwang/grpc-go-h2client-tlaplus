------------------------------ MODULE HTTP2Client ------------------------------
(***************************************************************************)
(* Formal model of grpc-go's internal/transport/http2_client.go focusing   *)
(* on transport lifecycle (reachable -> draining -> closing), stream quota *)
(* admission control in NewStream, GracefulClose, HandleGoAway, Close,     *)
(* CloseStream, and HandleSettings.                                        *)
(*                                                                         *)
(* Models issue https://github.com/grpc/grpc-go/issues/9559 and            *)
(* PR https://github.com/grpc/grpc-go/pull/9616.                           *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Callers,                  \* Set of goroutines calling NewStream (e.g. {c1, c2})
    InitMaxStreams,           \* Initial SETTINGS_MAX_CONCURRENT_STREAMS
    MaxStreamID,              \* Odd max stream ID (e.g. 3 or 5)
    FixMode,                  \* "none" | "local_flag" | "pr9616" | "full_fix"
    EnableSettingsUpdate,     \* TRUE/FALSE: allow SETTINGS increasing max streams
    EnableDirectClose,        \* TRUE/FALSE: allow two-phase Close() directly
    EnableStreamIDExhaustion  \* TRUE/FALSE: allow nextID > MaxStreamID drain trigger

VARIABLES
    \* Guarded by t.mu
    state,                    \* "reachable" | "draining" | "closing"
    activeStreams,            \* Subset of Callers with an established stream
    activeStreamsNil,         \* TRUE once Close() sets t.activeStreams = nil
    nextID,                   \* Next odd stream ID (1, 3, 5, ...)

    \* Channels / context on http2Client
    goAwayClosed,             \* t.goAway closed (by HandleGoAway)
    gracefulClosed,           \* t.gracefulClose closed (PR #9616, by GracefulClose)
    tCtxClosed,               \* t.ctx.Done() closed (by Close phase 2: t.cancel())
    controlBufClosed,         \* controlBuf.closed (by Close phase 2)

    \* Guarded by controlBuf.mu
    maxConcurrentStreams,     \* Current max concurrent streams
    streamQuota,              \* Current available stream quota (can be <= 0)
    waitingStreams,           \* Count of goroutines waiting for stream quota
    quotaChGen,               \* Generation of t.streamsQuotaAvailable; gen < quotaChGen are CLOSED
    quotaChToken,             \* Buffered(1) token in current generation of t.streamsQuotaAvailable

    \* Per-caller local variables in NewStream
    pc,                       \* [Callers -> {"idle","try","wait","needDrain","open","returned_err","closed_stream"}]
    chGen,                    \* [Callers -> {-1} \union Nat] (-1 represents Go's nil chan)
    firstTry,                 \* [Callers -> BOOLEAN]
    streamID                  \* [Callers -> Nat] (0 if no stream assigned)

NilChan == -1

vars == <<state, activeStreams, activeStreamsNil, nextID,
          goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
          maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen, quotaChToken,
          pc, chGen, firstTry, streamID>>

(***************************************************************************)
(* Type invariant                                                          *)
(***************************************************************************)
TypeOK ==
    /\ state \in {"reachable", "draining", "closing"}
    /\ activeStreams \subseteq Callers
    /\ activeStreamsNil \in BOOLEAN
    /\ nextID \in Nat /\ nextID % 2 = 1
    /\ goAwayClosed \in BOOLEAN
    /\ gracefulClosed \in BOOLEAN
    /\ tCtxClosed \in BOOLEAN
    /\ controlBufClosed \in BOOLEAN
    /\ maxConcurrentStreams \in Nat
    /\ streamQuota \in Int
    /\ waitingStreams \in Nat
    /\ quotaChGen \in Nat
    /\ quotaChToken \in BOOLEAN
    /\ pc \in [Callers -> {"idle", "try", "wait", "needDrain", "open", "returned_err", "closed_stream"}]
    /\ chGen \in [Callers -> (({NilChan} \union Nat))]
    /\ firstTry \in [Callers -> BOOLEAN]
    /\ streamID \in [Callers -> Nat]

(***************************************************************************)
(* Initial state                                                           *)
(***************************************************************************)
Init ==
    /\ state = "reachable"
    /\ activeStreams = {}
    /\ activeStreamsNil = FALSE
    /\ nextID = 1
    /\ goAwayClosed = FALSE
    /\ gracefulClosed = FALSE
    /\ tCtxClosed = FALSE
    /\ controlBufClosed = FALSE
    /\ maxConcurrentStreams = InitMaxStreams
    /\ streamQuota = InitMaxStreams
    /\ waitingStreams = 0
    /\ quotaChGen = 0
    /\ quotaChToken = FALSE
    /\ pc = [c \in Callers |-> "idle"]
    /\ chGen = [c \in Callers |-> NilChan]
    /\ firstTry = [c \in Callers |-> TRUE]
    /\ streamID = [c \in Callers |-> 0]

(***************************************************************************)
(* Helper: GracefulClose state transition                                  *)
(* Mirrors http2_client.go GracefulClose()                                 *)
(***************************************************************************)
DoGracefulClose ==
    IF state \in {"draining", "closing"} THEN
        UNCHANGED <<state, activeStreams, activeStreamsNil, gracefulClosed>>
    ELSE IF activeStreams = {} THEN
        \* active == 0 -> immediately enters Close() phase 1
        /\ state' = "closing"
        /\ activeStreamsNil' = TRUE
        /\ gracefulClosed' = (FixMode \in {"pr9616", "full_fix"})
        /\ UNCHANGED activeStreams
    ELSE
        \* active > 0 -> enters draining; does NOT close goAway or cancel t.ctx
        /\ state' = "draining"
        /\ gracefulClosed' = (FixMode \in {"pr9616", "full_fix"})
        /\ UNCHANGED <<activeStreams, activeStreamsNil>>

(***************************************************************************)
(* Actions                                                                 *)
(***************************************************************************)

\* Caller starts a NewStream(context.Background(), ...) call (no deadline)
StartNewStream(c) ==
    /\ pc[c] = "idle"
    /\ pc' = [pc EXCEPT ![c] = "try"]
    /\ firstTry' = [firstTry EXCEPT ![c] = TRUE]
    /\ chGen' = [chGen EXCEPT ![c] = NilChan]
    /\ UNCHANGED <<state, activeStreams, activeStreamsNil, nextID,
                   goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
                   maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen, quotaChToken,
                   streamID>>

\* Atomic execution of controlBuf.executeAndPut(checkForStreamQuota, hdr)
ExecuteCheckForStreamQuota(c) ==
    /\ pc[c] = "try"
    /\ IF controlBufClosed THEN
           \* controlBuf.executeAndPut returns ErrConnClosing
           /\ pc' = [pc EXCEPT ![c] = "returned_err"]
           /\ UNCHANGED <<state, activeStreams, activeStreamsNil, nextID,
                          goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
                          maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen, quotaChToken,
                          chGen, firstTry, streamID>>
       ELSE IF streamQuota <= 0 THEN
           \* No quota: record waiter, capture current streamsQuotaAvailable channel, enter select
           /\ waitingStreams' = IF firstTry[c] THEN waitingStreams + 1 ELSE waitingStreams
           /\ chGen' = [chGen EXCEPT ![c] = quotaChGen]
           /\ firstTry' = [firstTry EXCEPT ![c] = FALSE]
           /\ pc' = [pc EXCEPT ![c] = "wait"]
           /\ UNCHANGED <<state, activeStreams, activeStreamsNil, nextID,
                          goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
                          maxConcurrentStreams, streamQuota, quotaChGen, quotaChToken,
                          streamID>>
       ELSE IF state = "draining" \/ activeStreamsNil \/ (FixMode = "full_fix" /\ EnableStreamIDExhaustion /\ nextID > MaxStreamID) THEN
           \* Transport is draining, closing, or out of stream IDs while streamQuota > 0
           /\ IF FixMode \in {"pr9616", "full_fix"} THEN
                  \* PR #9616 & full_fix leave streamQuota & waitingStreams untouched
                  UNCHANGED <<streamQuota, waitingStreams>>
              ELSE
                  \* Before PR #9616: decremented before t.mu.Lock()
                  /\ waitingStreams' = IF ~firstTry[c] THEN waitingStreams - 1 ELSE waitingStreams
                  /\ streamQuota' = streamQuota - 1
           /\ IF FixMode \in {"local_flag", "full_fix"} THEN
                  \* Fail fast immediately instead of entering select with nil/closed ch
                  /\ pc' = [pc EXCEPT ![c] = "returned_err"]
                  /\ UNCHANGED firstTry
              ELSE
                  \* Returns false without updating ch -> enters select with ch unchanged
                  /\ firstTry' = [firstTry EXCEPT ![c] = FALSE]
                  /\ pc' = [pc EXCEPT ![c] = "wait"]
           /\ UNCHANGED <<state, activeStreams, activeStreamsNil, nextID,
                          goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
                          maxConcurrentStreams, quotaChGen, quotaChToken,
                          chGen, streamID>>
       ELSE
           \* Success: allocate stream ID and register in activeStreams
           /\ LET newWaiting == IF ~firstTry[c] THEN waitingStreams - 1 ELSE waitingStreams
                  newQuota   == streamQuota - 1
                  assignedID == nextID
                  newNextID  == nextID + 2
              IN
              /\ waitingStreams' = newWaiting
              /\ streamQuota' = newQuota
              /\ streamID' = [streamID EXCEPT ![c] = assignedID]
              /\ nextID' = newNextID
              /\ activeStreams' = activeStreams \union {c}
              /\ quotaChToken' = IF (newQuota > 0 /\ newWaiting > 0) THEN TRUE ELSE quotaChToken
              /\ pc' = [pc EXCEPT ![c] =
                    IF EnableStreamIDExhaustion /\ newNextID > MaxStreamID
                    THEN "needDrain"
                    ELSE "open"]
           /\ UNCHANGED <<state, activeStreamsNil,
                          goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
                          maxConcurrentStreams, quotaChGen, chGen, firstTry>>

\* NewStream line 972: after releasing locks, if nextID > MaxStreamID, call GracefulClose()
NewStreamTriggerDrain(c) ==
    /\ pc[c] = "needDrain"
    /\ DoGracefulClose
    /\ pc' = [pc EXCEPT ![c] = "open"]
    /\ UNCHANGED <<nextID, goAwayClosed, tCtxClosed, controlBufClosed,
                   maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen, quotaChToken,
                   chGen, firstTry, streamID>>

\* Select branch 1: `case <-ch:` (retries the for-loop)
NewStreamSelectRetry(c) ==
    /\ pc[c] = "wait"
    /\ chGen[c] >= 0
    /\ \/ \* Channel was closed by HandleSettings -> always ready, does not consume current token
          /\ chGen[c] < quotaChGen
          /\ UNCHANGED quotaChToken
       \/ \* Current channel has a buffered token -> consume it
          /\ chGen[c] = quotaChGen
          /\ quotaChToken
          /\ quotaChToken' = FALSE
    /\ pc' = [pc EXCEPT ![c] = "try"]
    /\ UNCHANGED <<state, activeStreams, activeStreamsNil, nextID,
                   goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
                   maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen,
                   chGen, firstTry, streamID>>

\* Select branches 2/3/4: `<-t.goAway`, `<-t.gracefulClose`, `<-t.ctx.Done()`
NewStreamSelectExit(c) ==
    /\ pc[c] = "wait"
    /\ \/ goAwayClosed
       \/ (FixMode \in {"pr9616", "full_fix"} /\ gracefulClosed)
       \/ tCtxClosed
    /\ pc' = [pc EXCEPT ![c] = "returned_err"]
    /\ UNCHANGED <<state, activeStreams, activeStreamsNil, nextID,
                   goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
                   maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen, quotaChToken,
                   chGen, firstTry, streamID>>

\* External GracefulClose() call (e.g., addrConn.updateAddrs)
GracefulClose ==
    /\ state = "reachable"
    /\ DoGracefulClose
    /\ UNCHANGED <<nextID, goAwayClosed, tCtxClosed, controlBufClosed,
                   maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen, quotaChToken,
                   pc, chGen, firstTry, streamID>>

\* Server sends a GOAWAY frame -> handleGoAway()
HandleGoAway ==
    /\ state # "closing"
    /\ ~goAwayClosed
    /\ goAwayClosed' = TRUE
    /\ IF activeStreams = {} THEN
           /\ state' = "closing"
           /\ activeStreamsNil' = TRUE
       ELSE
           /\ state' = "draining"
           /\ UNCHANGED activeStreamsNil
    /\ UNCHANGED <<activeStreams, nextID, gracefulClosed, tCtxClosed, controlBufClosed,
                   maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen, quotaChToken,
                   pc, chGen, firstTry, streamID>>

\* Active stream closes normally -> closeStream() + loopyWriter cleanupStreamHandler()
\* NOTE: Intentionally NOT fair so long-lived streams can remain open forever!
CloseStream(c) ==
    /\ pc[c] = "open"
    /\ c \in activeStreams
    /\ LET rem == activeStreams \ {c}
       IN
       /\ activeStreams' = rem
       /\ pc' = [pc EXCEPT ![c] = "closed_stream"]
       /\ IF ~controlBufClosed THEN
              /\ streamQuota' = streamQuota + 1
              /\ quotaChToken' = IF (streamQuota + 1 > 0 /\ waitingStreams > 0) THEN TRUE ELSE quotaChToken
          ELSE
              UNCHANGED <<streamQuota, quotaChToken>>
       /\ IF state = "draining" /\ rem = {} THEN
              /\ state' = "closing"
              /\ activeStreamsNil' = TRUE
          ELSE
              UNCHANGED <<state, activeStreamsNil>>
    /\ UNCHANGED <<nextID, goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
                   maxConcurrentStreams, waitingStreams, quotaChGen,
                   chGen, firstTry, streamID>>

\* Direct Close(err) Phase 1 (e.g., reader error / keepalive):
\* Sets state = closing and activeStreams = nil, BEFORE waiting for writerDone and calling t.cancel()
DirectClosePhase1 ==
    /\ EnableDirectClose
    /\ state # "closing"
    /\ state' = "closing"
    /\ activeStreamsNil' = TRUE
    /\ activeStreams' = {}
    /\ gracefulClosed' = IF (FixMode = "full_fix" /\ state # "draining") THEN TRUE ELSE gracefulClosed
    /\ UNCHANGED <<nextID, goAwayClosed, tCtxClosed, controlBufClosed,
                   maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen, quotaChToken,
                   pc, chGen, firstTry, streamID>>

\* Close(err) Phase 2: writerDone or 5s timeout fires -> t.cancel() and controlBuf.finish()
CloseFinish ==
    /\ state = "closing"
    /\ (~tCtxClosed \/ ~controlBufClosed)
    /\ tCtxClosed' = TRUE
    /\ controlBufClosed' = TRUE
    /\ UNCHANGED <<state, activeStreams, activeStreamsNil, nextID,
                   goAwayClosed, gracefulClosed,
                   maxConcurrentStreams, streamQuota, waitingStreams, quotaChGen, quotaChToken,
                   pc, chGen, firstTry, streamID>>

\* Server sends SETTINGS increasing MaxConcurrentStreams by 1 (bounded to 1 update)
HandleSettingsIncrease ==
    /\ EnableSettingsUpdate
    /\ ~controlBufClosed
    /\ maxConcurrentStreams < InitMaxStreams + 1
    /\ maxConcurrentStreams' = maxConcurrentStreams + 1
    /\ streamQuota' = streamQuota + 1
    /\ IF waitingStreams > 0 THEN
           \* close(t.streamsQuotaAvailable); t.streamsQuotaAvailable = make(chan struct{}, 1)
           /\ quotaChGen' = quotaChGen + 1
           /\ quotaChToken' = FALSE
       ELSE
           UNCHANGED <<quotaChGen, quotaChToken>>
    /\ UNCHANGED <<state, activeStreams, activeStreamsNil, nextID,
                   goAwayClosed, gracefulClosed, tCtxClosed, controlBufClosed,
                   waitingStreams, pc, chGen, firstTry, streamID>>

Next ==
    \/ \E c \in Callers :
           \/ StartNewStream(c)
           \/ ExecuteCheckForStreamQuota(c)
           \/ NewStreamTriggerDrain(c)
           \/ NewStreamSelectRetry(c)
           \/ NewStreamSelectExit(c)
           \/ CloseStream(c)
    \/ GracefulClose
    \/ HandleGoAway
    \/ DirectClosePhase1
    \/ CloseFinish
    \/ HandleSettingsIncrease

\* Fairness:
\* - Internal goroutine progress is weakly fair.
\* - Go's randomized select between ready exit & retry branches is modeled via SF on Exit.
\* - CloseStream is intentionally UNFAIR (long-lived streams may stay open forever).
Fairness ==
    /\ \A c \in Callers :
           /\ WF_vars(ExecuteCheckForStreamQuota(c))
           /\ WF_vars(NewStreamTriggerDrain(c))
           /\ WF_vars(NewStreamSelectRetry(c))
           /\ SF_vars(NewStreamSelectExit(c))
    /\ WF_vars(CloseFinish)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties to check                                                     *)
(***************************************************************************)

\* Core Liveness Property (Contract of ClientTransport.GracefulClose):
\* Once the transport is draining or closing, every in-flight or waiting
\* NewStream call must eventually return (without requiring existing
\* long-lived streams to close).
NewStreamTerminatesOnDrain ==
    \A c \in Callers :
        []( (pc[c] \in {"try", "wait"} /\ state \in {"draining", "closing"})
            ~> (pc[c] \in {"open", "returned_err", "closed_stream"}) )

\* Extra Safety Invariant 1: No busy-spin loop in NewStream during Close()
\* before t.cancel() runs (caused by holding an old closed streamsQuotaAvailable
\* channel that wasn't cleared when checkForStreamQuota saw activeStreams == nil).
NoBusySpinDuringClose ==
    \A c \in Callers :
        ~( pc[c] = "wait"
           /\ state = "closing"
           /\ ~tCtxClosed
           /\ ~goAwayClosed
           /\ ~(FixMode \in {"pr9616", "full_fix"} /\ gracefulClosed)
           /\ chGen[c] >= 0
           /\ chGen[c] < quotaChGen )

\* Extra Safety Invariant 2: No stream is ever assigned an ID > MaxStreamID
ValidStreamIDs ==
    \A c \in Callers : streamID[c] <= MaxStreamID

=============================================================================
