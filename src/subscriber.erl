
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
    Pid = proc_lib:spawn_link(?MODULE, accept_snapshot_loop, [?MODULE, Snapshot]),  
    io:format("s:accept_snapshot:PID:~p~n", [Pid]),
    State.  

accept_sub(State =#server_state{subscriber_socket=Subscriber}) ->  
    ok = erlzmq:setsockopt(Subscriber, subscribe, <<"B">>),
    Pid = proc_lib:spawn_link(?MODULE, accept_sub_loop, [?MODULE, Subscriber]),
    io:format("s:accept_sub:PID:~p~n", [Pid]),
    State.  

accept_snapshot_loop(Server, _SnapshotSocket) ->  
    io:format("s::>>>>accept_snapshot_loop<<<~n", []),
    ok = gen_server:cast(?MODULE, {accept_snapshot_new, Server}),
    io:format("s::>>>> end of accept_snapshot_loop<<<~n", []).
    
accept_sub_loop(Server, _SubscriberSocket) ->  
    gen_server:cast(?MODULE, {accept_sub_new, Server}).

process_more_snapshot({ok, V}, State=#server_state{snapshot_socket=Socket, heartbeat_at=HeartbeatAt}) -> 
  io:format("s:comming..................~n",[]),
  case erlzmq:getsockopt(Socket, events) of
    {ok, ?TNC_ZMQ_POLLIN} ->
      Now = system_time:get_timestamp(),
      io:format("Now:~p~n", [Now]),
      io:format("s: consume queue messages.~n", []),
      case erlzmq:recv(Socket) of 
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
          State;
        _ ->
          io:format("cat others.", []),
          State
      end;
    {ok, ?TNC_ZMQ_POLLOUT} ->
      Now = system_time:get_timestamp(),
      case (Now - HeartbeatAt) > 10 of
        true ->
          io:format("s:server lost connection.------------ NOW:~p=HeartbeatAt:~p~n",[Now, HeartbeatAt]);
        _ ->
          do_nothing
      end,
      timer:sleep(?LOOP_SLEEP),
      State;
    _ ->
      io:format("s:strange~n",[]),
      State
  end;      
  
process_more_snapshot(_,  State) ->
  io:format("s:late..................~n",[]),
  State.
  
handle_cast({accept_snapshot_new, FromPid}, State=#server_state{snapshot_socket=SnapshotSocket})->
  io:format("s:accept_snapshot_new(FromPid:~p).~n", [FromPid]),
  NewState = process_more_snapshot(erlzmq:getsockopt(SnapshotSocket, rcvmore), State),
  io:format("going to loop accept_snapshot_loop.~n", []),
  Pid = proc_lib:spawn_link(subscriber, accept_snapshot_loop, [?MODULE, SnapshotSocket]),
  io:format("s:pid:~p.self():~p~n", [Pid, ?MODULE]),
  {noreply, NewState};
  
  
handle_cast({accept_sub_new, _FromPid}, State=#server_state{subscriber_socket=SubscriberSocket})->
  {ok, _Key} = erlzmq:recv(SubscriberSocket),
  % io:format("s::accept_sub_new Key:~p~n", [Key]),
  %% Read message contents
  {ok, _Value} = erlzmq:recv(SubscriberSocket),
  % io:format("s::accept_sub_new [~s] ~s~n", [Key, Value]),

  Pid = proc_lib:spawn_link(subscriber, accept_sub_loop, [?MODULE, SubscriberSocket]),
  io:format("s:accept_sub_new:PID:~p~n", [Pid]),
  {noreply, State};
    
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
terminate(normal, _State) ->
  io:format("s:terminate..................~n",[]),
    ok.