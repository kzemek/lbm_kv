%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @copyright (C) 2014, Lindenbaum GmbH
%%%
%%% Permission to use, copy, modify, and/or distribute this software for any
%%% purpose with or without fee is hereby granted, provided that the above
%%% copyright notice and this permission notice appear in all copies.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%%%
%%% @doc
%%% Provides simple, Mnesia-based, distributed key value tables. When started,
%%% this application distributes Mnesia (and all `lbm_kv' tables) over all
%%% dynamically connected nodes. The Mnesia cluster can grow and shrink
%%% dynamically.
%%%
%%% All tables created, have key/value semantic (after all its still Mnesia).
%%% A new key-value-table can be created using {@link create/1}. The table will
%%% automatically be replicated to other nodes as new node connections are
%%% detected. Every connected node has read and write access to all Mnesia
%%% tables. If desired, it is possible to use the default Mnesia API to
%%% manipulate `lbm_kv' tables. However, `lbm_kv' uses vector clocks that need
%%% to be updated on every write to be able to use automatic netsplit recovery!
%%% Use the `#lbm_kv{}' record from the `lbm_kv.hrl' header file to match
%%% `lbm_kv' table entries.
%%%
%%% Every `lbm_kv' table uses vector clocks to keep track of the its entries.
%%% In case of new node connections or netsplits, `lbm_kv' will use these to
%%% merge the island itself without interaction. However, if there are diverged
%%% entries `lbm_kv' will look for a user defined callback to resolve the
%%% conflict. If no such callback can be found one of the conflicting nodes will
%%% be restarted!
%%%
%%% To be able to use `lbm_kv' none of the connected nodes is allowed to have
%%% `disk_copies' of its `schema' table, because Mnesia will fail to merge
%%% schemas on disk nodes (which means that it is likely they can't
%%% participate). If you need `disk_copies', you're on your own here. Do not
%%% mess with table replication and mnesia configuration changes yourself!
%%% There's a lot of black magic happening inside Mnesia and `lbm_kv' will do
%%% the necessary tricks and workarounds for you. At best you should avoid
%%% having tables created from outside `lbm_kv'. At least do not create tables
%%% with conflicting names.
%%% @end
%%%=============================================================================

-module(lbm_kv).

-behaviour(application).
-behaviour(supervisor).

%% API
-export([create/1, info/0]).

%% Application callbacks
-export([start/2, stop/1]).

%% supervisor callbacks
-export([init/1]).

-type table() :: atom().

-export_type([table/0]).

-include("lbm_kv.hrl").

%%%=============================================================================
%%% Behaviour
%%%=============================================================================

-callback attributes() -> [atom()].
-callback handle_conflict(Key :: term(), Local :: term(), Remote :: term()) ->
    {value, term()} | delete | term().
%% An optional callback that will be called on the node performing a table
%% merge (usually an arbitrary node) whenever an entry of table cannot be
%% merged automatically. The callback must be implemented in a module with the
%% same name as the respective table name, e.g. to handle conflicts for values
%% in the table `my_table' the module/function `my_table:handle_conflict/3' has
%% to be implemented.
%%
%% The function can resolve conflicts in several ways. It can provide a (new)
%% value for `Key' by returning `{value, Val}', it can delete all associations
%% for `Key' on all nodes by returning `delete' or it can ignore the
%% inconsistency by returning anything else or crash. When ignoring an
%% inconsistency the values for key will depend on the location of retrieval
%% until a new value gets written for `Key'.
%%
%% If an appropriate callback is not provided, the default conflict resolution
%% strategy is to __restart__ one of the conflicting node islands!

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Create a new key value table which will be replicated as RAM copy across all
%% nodes in the cluster. The table will only be created, if not yet existing.
%% This can be called multiple times (even) on the same node.
%%
%% The table will be ready for reads and writes when this function returns.
%% @end
%%------------------------------------------------------------------------------
-spec create(table()) -> ok | {error, term()}.
create(Table) ->
    case mnesia:create_table(Table, ?LBM_KV_TABLE_OPTS(Table)) of
        {atomic, ok} ->
            await_table(Table);
        {aborted, {already_exists, Table}} ->
            await_table(Table);
        {aborted, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Print information about the `lbm_kv' state to stdout.
%% @end
%%------------------------------------------------------------------------------
-spec info() -> ok.
info() -> lbm_kv_mon:info().

%%%=============================================================================
%%% Application callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start(_StartType, _StartArgs) -> supervisor:start_link(?MODULE, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
stop(_State) -> ok.

%%%=============================================================================
%%% supervisor callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) -> {ok, {{one_for_one, 5, 1}, [spec(lbm_kv_mon, [])]}}.

%%%=============================================================================
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
spec(M, As) -> {M, {M, start_link, As}, permanent, 1000, worker, [M]}.

%%------------------------------------------------------------------------------
%% @private
%% Blocks the calling process until a certain table is available to this node.
%%------------------------------------------------------------------------------
await_table(Table) ->
    Timeout = application:get_env(?MODULE, wait_timeout, 10000),
    case mnesia:wait_for_tables([Table], Timeout) of
        ok ->
            ok;
        {timeout, [Table]} ->
            {error, timeout};
        Error ->
            Error
    end.
