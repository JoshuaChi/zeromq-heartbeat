
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
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->  
    process_flag(trap_exit, true),
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
    _Pid = proc_lib:spawn_link(?MODULE, accept_snapshot_loop, [?MODULE, Snapshot]),
    State.  

accept_sub(State =#server_state{subscriber_socket=Subscriber}) ->  
    ok = erlzmq:setsockopt(Subscriber, subscribe, ?ROUTER_WIO),
    _Pid = proc_lib:spawn_link(?MODULE, accept_sub_loop, [?MODULE, Subscriber]),
    State.  

accept_snapshot_loop(Server, SnapshotSocket) ->  
    ok = gen_server:cast(?MODULE, {accept_snapshot_new, Server}),
    accept_snapshot_loop(Server, SnapshotSocket).
    
accept_sub_loop(Server, _SubscriberSocket) ->  
    gen_server:cast(?MODULE, {accept_sub_new, Server}).
  
handle_cast({accept_snapshot_new, _FromPid}, State=#server_state{snapshot_socket=SnapshotSocket})->
  NewState = process_more_snapshot(erlzmq:getsockopt(SnapshotSocket, rcvmore), State),
  % io:format("going to loop accept_snapshot_loop.~n", []),
  % Pid = proc_lib:spawn_link(subscriber, accept_snapshot_loop, [?MODULE, SnapshotSocket]),
  % io:format("s:pid:~p.self():~p~n", [Pid, ?MODULE]),
  {noreply, NewState};
  
  
handle_cast({accept_sub_new, _FromPid}, State=#server_state{subscriber_socket=SubscriberSocket})->
  Now = system_time:get_timestamp(),
  NewState = case erlzmq:recv(SubscriberSocket, [dontwait]) of
    {ok, Key} ->
      io:format("s::accept_sub_new (ok:~p)~n", [Key]),
      State#server_state{heartbeat_at = (Now + ?HEARTBEAT_DELAY)};
    {error, Reason} ->
      io:format("s::accept_sub_new (error:~p)~n", [Reason]),
      State;
    _ ->
      State
  end,
  
  case erlzmq:recv(SubscriberSocket, [dontwait]) of
    {ok, Value} ->
      io:format("s::accept_sub_new (ok:~p)~n", [Value]);
    {error, Reason2} ->
      io:format("s::accept_sub_new (error:~p)~n", [Reason2]);
    _ ->
      other
  end,

  timer:sleep(?LOOP_SLEEP),
  _Pid = proc_lib:spawn_link(subscriber, accept_sub_loop, [?MODULE, SubscriberSocket]),
  {noreply, NewState};
    
handle_cast(Msg, State) ->
  io:format("s:handle_cast..................Msg:~p.~n",[Msg]),
  {noreply, State}.

handle_call(stop, _From, State) ->
    io:format("s:handle_call::stop..................~n",[]),
    {stop, normal, ok, State};
handle_call(_Req, _From, State) ->
    io:format("s:handle_call..................~n",[]),
    {reply, {error, badarg}, State}.

handle_info({'EXIT', Pid, Info}, State) ->
  io:format("s:handle_info::'EXIT'(Pid:~p, Info:~p)..................~n",[Pid, Info]),
  {noreply, State};
handle_info({'DOWN', MonitorRef, Type, Obj, Info}, State) ->
  io:format("s:handle_info::'DOWN'(MonitorRef:~p, Type:~p, Obj:~p, Info:~p)..................~n",[MonitorRef, Type, Obj, Info]),
  {noreply, State};      
handle_info(_Info, State) ->
  io:format("s:handle_info..................~n",[]),
  {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
  io:format("s:code_change..................~n",[]),
  {ok, State}.
terminate(normal, _State=#server_state{zmq_context=Context}) ->
  ok = erlzmq:term(Context),
  io:format("s:terminate..................~n",[]),
  ok.
  
  


process_more_snapshot({ok, _V}, State=#server_state{snapshot_socket=Socket, heartbeat_at=HeartbeatAt}) -> 
  case erlzmq:getsockopt(Socket, events) of
    {ok, ?TNC_ZMQ_POLLIN} ->
      Now = system_time:get_timestamp(),
      % io:format("Now:~p~n", [Now]),
      case erlzmq:recv(Socket) of 
        {ok, ?HEARTBEAT_CMD} ->
          % io:format("s:Get server heart beat, server is still alive.~n", []),
          State#server_state{heartbeat_at = (Now + ?HEARTBEAT_DELAY)};
        {ok, ?SNAPSHOT_ACK} ->
          % io:format("s:now we should handle sequences.~n", []),
          State;
        {ok, _Value} ->
          % io:format("s::process_more_snapshot V:~p:Value:~p~n", [V, Value]),
          process_more_snapshot(erlzmq:getsockopt(Socket, rcvmore), Socket),
          State;
        {error, _Reason}->
          State;
        _ ->
          % io:format("cat others.", []),
          State
      end;
    {ok, ?TNC_ZMQ_POLLOUT} ->
      Now = system_time:get_timestamp(),
      case (Now - HeartbeatAt) > 10 of
        true ->
          io:format("s:server lost connection(NOW - HeartbeatAt) = ~p~n",[(Now - HeartbeatAt)]);
        _ ->
          do_nothing
      end,
      timer:sleep(?LOOP_SLEEP),
      State;
    _ ->
      State
  end;      
  
process_more_snapshot(_,  State) ->
  State.  