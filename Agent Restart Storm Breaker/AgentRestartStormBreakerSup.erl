%%% AgentRestartStormBreakerSup
%%%
%%% Ordinary OTP supervisor wrapping the breaker gen_server. The breaker is
%%% designed to never crash from a task's failure (workers are unlinked and
%%% only ever communicate back via monitor/cast), so this supervisor exists
%%% for the residual case of a genuine bug in the breaker itself: a restart
%%% here resets the ledger and storm state to a safe, fail-open default
%%% (empty ledger, storm_state = closed) rather than losing task isolation
%%% for the process's whole lifetime.
-module('AgentRestartStormBreakerSup').
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 60},
    ChildSpec = #{
        id => 'AgentRestartStormBreaker',
        start => {'AgentRestartStormBreaker', start_link, ['AgentRestartStormBreaker', #{}]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => ['AgentRestartStormBreaker']
    },
    {ok, {SupFlags, [ChildSpec]}}.
