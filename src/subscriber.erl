
-module(subscriber).

-include("heartbeat.hrl").

-import(system_time, [get_timestamp/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/0, accept_snapshot_loop/2, accept_sub_loop/2, process_more_snapshot/2]).


-record(server_state, {  
  zmq_context,
  snapshot_socket,
  subscriber_socket,
  heartbeat_at
}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen behavior 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link() ->
    gen_server:start_link({local, subscriber}, subscriber, [], []).

init([]) ->  
    case erlzmq:context() of
      {ok, Context} ->
        {ok, State} = case erlzmq:socket(Context, [dealer]) of 
          {ok, Snapshot} ->
            ok = erlzmq:setsockopt(Snapshot, identity, <<"solr">>),
            case erlzmq:connect(Snapshot, "tcp://localhost:5570") of
              ok ->
                erlzmq:send(Snapshot, <<"ICANHAZ?">>),
                HeartbeatAt = system_time:get_timestamp() + ?HEARTBEAT_DELAY,
                {ok, accept_snapshot(#server_state{zmq_context=Context, snapshot_socket=Snapshot, heartbeat_at=HeartbeatAt})};
              _ ->
                {stop, failed_connect_zmq_5570}
            end;
          _ ->
            {stop, failed_create_dealer_socket}
        end,
        
        case erlzmq:socket(Context, [sub]) of 
          {ok, Subscriber} ->
            case erlzmq:connect(Subscriber, "tcp://localhost:5571") of
              ok ->
                ok = erlzmq:setsockopt(Subscriber, subscribe, ?ROUTER_WIO),
                {ok, accept_sub(State#server_state{subscriber_socket=Subscriber})};
              _ ->
                {stop, failed_connect_zmq_5571}
            end;
          _ ->
            {stop, failed_create_sub_socket}
        end;
            
      _ ->
        {stop, fail_to_get_zmq_context}
    end.


accept_snapshot(State =#server_state{snapshot_socket=Snapshot}) ->
    proc_lib:spawn(subscriber, accept_snapshot_loop, [self(), Snapshot]),  
    State.  

accept_sub(State =#server_state{subscriber_socket=Subscriber}) ->  
    ok = erlzmq:setsockopt(Subscriber, subscribe, <<"B">>),
    proc_lib:spawn(subscriber, accept_sub_loop, [self(), Subscriber]),
    State.  

accept_snapshot_loop(Server, _SnapshotSocket) ->  
    % io:format("s::>>>>accept_snapshot_loop<<<~n", []),
    gen_server:cast(Server, {accept_snapshot_new, self()}).
    
accept_sub_loop(Server, _SubscriberSocket) ->  
    gen_server:cast(Server, {accept_sub_new, self()}).

process_more_snapshot({ok, V}, State=#server_state{snapshot_socket=Socket, heartbeat_at=HeartbeatAt}) ->
  Now = system_time:get_timestamp(),
  io:format("Now:~p~n", [Now]),
  case erlzmq:recv(Socket, [dontwait]) of 
    {ok, ?HEARTBEAT_CMD} ->
      io:format("s:Get server heart beat, server is still alive.~n", []),
      State#server_state{heartbeat_at = (Now + ?HEARTBEAT_DELAY)};
    {ok, ?SNAPSHOT_ACK} ->
      io:format("s:now we should handle sequences.~n", []),
      State;
    {ok, Value} ->
      io:format("s::process_more_snapshot V:~p:Value:~p~n", [V, Value]),
      process_more_snapshot(erlzmq:getsockopt(Socket, rcvmore), Socket),
      State;
    {error, _Reason}->
      case (Now - HeartbeatAt) > 10 of
        true ->
          io:format("s:server lost connection.------------ NOW:~p=HeartbeatAt:~p~n",[Now, HeartbeatAt]);
        _ ->
          do_nothing
      end,
      State;
    _ ->
      io:format("cat others.", []),
      State
  end;
  
process_more_snapshot(_,  State) ->
  State.
  
handle_cast({accept_snapshot_new, _FromPid}, State=#server_state{snapshot_socket=SnapshotSocket})->
  io:format("S.....~n", []),
  NewState = process_more_snapshot(erlzmq:getsockopt(SnapshotSocket, rcvmore), State),
  
  timer:sleep(?LOOP_SLEEP),
  _Pid = proc_lib:spawn(subscriber, accept_snapshot_loop, [self(), SnapshotSocket]),
  {noreply, NewState};
  
  
handle_cast({accept_sub_new, _FromPid}, State=#server_state{subscriber_socket=SubscriberSocket})->
  {ok, _Key} = erlzmq:recv(SubscriberSocket),
  % io:format("s::accept_sub_new Key:~p~n", [Key]),
  %% Read message contents
  {ok, _Value} = erlzmq:recv(SubscriberSocket),
  % io:format("s::accept_sub_new [~s] ~s~n", [Key, Value]),
  
  _Pid = proc_lib:spawn(subscriber, accept_sub_loop, [self(), SubscriberSocket]),
  {noreply, State};
    
handle_cast(_Msg, State) ->
  {noreply, State}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badarg}, State}.
      
handle_info(_Info, State) ->
  {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
terminate(normal, _State) ->
    ok.