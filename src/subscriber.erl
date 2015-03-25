
-module(subscriber).

-include("heartbeat.hrl").

-import(system_time, [get_timestamp/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/0, accept_snapshot_loop/2, accept_sub_loop/2]).


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
            ok = erlzmq:setsockopt(Snapshot, identity, pid_to_list(self())),
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
                ok = erlzmq:setsockopt(Subscriber, subscribe, <<"B">>),
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

handle_cast({accept_snapshot_new, _FromPid}, State=#server_state{snapshot_socket=SnapshotSocket})->
  {ok, Identify} = erlzmq:recv(SnapshotSocket),
  io:format("s::accept_snapshot_new Identify:~p~n", [Identify]),
  %% Read message contents
  
  case erlzmq:recv(SnapshotSocket, [dontwait]) of
    {ok, ?HEARTBEAT_CMD} ->
      io:format("s:Get client heart beat, client is still alive.~n", []);
    {ok, Contents} ->
      io:format("s::accept_snapshot_new [~s] ~s~n", [Identify, Contents]);
    _ ->
      nothing
  end,
  
  _Pid = proc_lib:spawn(subscriber, accept_snapshot_loop, [self(), SnapshotSocket]),
  {noreply, State};
  
  
handle_cast({accept_sub_new, _FromPid}, State=#server_state{subscriber_socket=SubscriberSocket})->
  {ok, Address} = erlzmq:recv(SubscriberSocket),
  io:format("s::accept_sub_new Address:~p~n", [Address]),
  %% Read message contents
  {ok, Contents} = erlzmq:recv(SubscriberSocket, [dontwait]),
  io:format("s::accept_sub_new [~s] ~s~n", [Address, Contents]),
  
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




% main() ->
%
%     %% Prepare our context and subscriber
%     {ok, Context} = erlzmq:context(),
%
%     {ok, Snapshot} = erlzmq:socket(Context, dealer),
%     ok = erlzmq:setsockopt(Snapshot, identity, pid_to_list(self())),
%     ok = erlzmq:connect(Snapshot, "tcp://localhost:5570"),
%     erlzmq:send(Snapshot, <<"ICANHAZ?">>),
%
%     {ok, Subscriber} = erlzmq:socket(Context, sub),
%     ok = erlzmq:connect(Subscriber, "tcp://localhost:5571"),
%     ok = erlzmq:setsockopt(Subscriber, subscribe, <<"B">>),
%
%
%     loop(Snapshot, Subscriber),
%
%     ok = erlzmq:term(Context).
%
%
% loop(Snapshot, Subscriber) ->
%     process_tasks(Snapshot),
%     process_tasks(Subscriber),
%     loop(Snapshot, Subscriber).
%
% process_tasks(Socket) ->
%     %% Read envelope with address
%     {ok, Address} = erlzmq:recv(Socket),
%     io:format("Address:~p~n", [Address]),
%     %% Read message contents
%     {ok, Contents} = erlzmq:recv(Socket),
%     io:format("[~s] ~s~n", [Address, Contents]).

% client_loop(Client, RequestNbr) ->
%     %% Tick once per second, pulling in arriving messages (check 100 times
%     %% using 10 poll delay for each call)
%     client_check_messages(Client, 100, 10),
%     Msg = list_to_binary(io_lib:format("request #~b", [RequestNbr])),
%     erlzmq:send(Client, Msg),
%     client_loop(Client, RequestNbr + 1).
%
% client_check_messages(_Client, 0, _PollDelay) -> ok;
% client_check_messages(Client, N, PollDelay) when N > 0 ->
%     case erlzmq:recv(Client, [noblock]) of
%         {ok, Msg} -> io:format("~s [~p]~n", [Msg, self()]);
%         {error, eagain} -> timer:sleep(PollDelay)
%     end,
%     client_check_messages(Client, N - 1, PollDelay).