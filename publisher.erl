%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(publisher).

-behaviour(gen_server).

-include("heartbeat.hrl").


-import(system_time, [get_timestamp/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/0, accept_snapshot_loop/2, accept_pub_loop/2]).


-record(server_state, {  
  zmq_context,
  snapshot_socket,
  publisher_socket,
  heartbeat_at
}).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen behavior 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link() ->
    gen_server:start_link({local, publisher}, publisher, [], []).

init(State) ->
    case erlzmq:context() of
      {ok, Context} ->
        {ok, NewState} = case erlzmq:socket(Context, [router]) of 
          {ok, Snapshot} ->
            case erlzmq:bind(Snapshot, "tcp://*:5570") of
              ok ->
                HeartbeatAt = system_time:get_timestamp() + ?HEARTBEAT_DELAY,
                {ok, accept_snapshot(State#server_state{zmq_context=Context, snapshot_socket=Snapshot, heartbeat_at=HeartbeatAt})};
              _ ->
                {stop, failed_bind_zmq_5570}
            end;
          _ ->
            {stop, failed_create_router_socket}
        end,
        
        case erlzmq:socket(Context, [pub]) of 
          {ok, Publisher} ->
            case erlzmq:bind(Publisher, "tcp://*:5571") of
              ok ->
                {ok, accept_pub(NewState#server_state{publisher_socket=Publisher})};
              _ ->
                {stop, failed_bind_zmq_5571}
            end;
          _ ->
            {stop, failed_create_pub_socket}
        end;
            
      _ ->
        {stop, fail_to_get_zmq_context}
    end.


accept_snapshot(State =#server_state{snapshot_socket=Snapshot}) ->  
    proc_lib:spawn(publisher, accept_snapshot_loop, [self(), Snapshot]),  
    State.  

accept_pub(State =#server_state{publisher_socket=Publisher}) ->  
    proc_lib:spawn(publisher, accept_pub_loop, [self(), Publisher]),  
    State.  

accept_snapshot_loop(Server, _SnapshotSocket) ->  
    gen_server:cast(Server, {accept_snapshot_new,self()}).

accept_pub_loop(Server, _PublisherSocket) ->  
    gen_server:cast(Server, {accept_pub_new,self()}).

handle_cast({accept_snapshot_new, _FromPid}, State=#server_state{snapshot_socket=SnapshotSocket, heartbeat_at=HeartbeatAt})->
  {ok, Identify} = erlzmq:recv(SnapshotSocket),
  
  NewState = case erlzmq:recv(SnapshotSocket) of
    {ok, ?SNAPSHOT} ->
      % ok = erlzmq:setsockopt(Snapshot, identity, pid_to_list(self())),
      ok = erlzmq:send(SnapshotSocket, Identify, [sndmore]),
      ok = erlzmq:send(SnapshotSocket, <<"89">>),
      ok = erlzmq:send(SnapshotSocket, Identify, [sndmore]),
      ok = erlzmq:send(SnapshotSocket, ?SNAPSHOT_ACK),
      io:format("Publish snamshots.~n", []),
      State;
    _ ->
      State
  end,

  Now = system_time:get_timestamp(),
  NewState2 = case Now > HeartbeatAt of
    true ->
      ok = erlzmq:send(SnapshotSocket, ?HEARTBE_CMD),
      NewState#server_state{heartbeat_at=Now};
    _ ->
      NewState
  end,
  
  _Pid = proc_lib:spawn(publisher, accept_snapshot_loop, [self(), SnapshotSocket]),
  {noreply, NewState2};

handle_cast({accept_pub_new, _FromPid},State=#server_state{publisher_socket=PublisherSocket})->  
  case erlzmq:recv(PublisherSocket) of
    % if get heartbeat command, it means client still alive, we do nothing
    {ok, ?HEARTBE_CMD} ->
      % HeartbeatAt = system_time:get_timestamp() + ?HEARTBEAT_DELAY,
      io:format("get heartbeat command.~n", []);
      % State#server_state{heartbeat_at=HeartbeatAt};
    _ ->
      ignore_others_for_now
  end,
  
  % send pubs,
  ok = erlzmq:send(PublisherSocket, <<"A">>, [sndmore]),
  ok = erlzmq:send(PublisherSocket, <<"We don't want to see this">>),
  ok = erlzmq:send(PublisherSocket, <<"B">>, [sndmore]),
  ok = erlzmq:send(PublisherSocket, <<"We would like to see this">>),  
  
  _Pid = proc_lib:spawn(publisher, accept_pub_loop, [self(), PublisherSocket]),
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

            
            
% Heaatbeat is two way communications. 
% Client send heartbeat every second if there is no message receive;[ROUTER]
% Server send heartbeat every second if there is no message send;[DEALER]
% Client will treat server died if no message or heartbeat received during heartbeat liveness;
% Server will treat client not connected if no message or heartbeat received during heartbeat liveness; It can try reconnect or do anything.
% // If liveness hits zero, queue is considered disconnected
% liveness = HEARTBEAT_LIVENESS;
% heartbeatAt = System.currentTimeMillis() + heartbeat;

% main() ->
%     %% Prepare our context and publisher
%     {ok, Context} = erlzmq:context(),
%     % But before that, what about gen_udp:open/2? The second argument can be a list of options, specifying in what type we want to receive data (list or binary), how we want them received; as messages ({active, true}) or as results of a function call ({active, false}). There are more options such as whether the socket should be set with IPv4 (inet4) or IPv6 (inet6), whether the UDP socket can be used to broadcast information ({broadcast, true | false}), the size of buffers, etc. There are more options available, but we'll stick to the simple stuff for now because understanding the rest is rather up to you to learn. The topic can become complex fast and this guide is about Erlang, not TCP and UDP, unfortunately.
%     % {ok, Snapshot} = erlzmq:socket(Context, [router, {active, true}]),
%     {ok, Snapshot} = erlzmq:socket(Context, [router]),
%     ok = erlzmq:bind(Snapshot, "tcp://*:5570"),
%
%     {ok, Publisher} = erlzmq:socket(Context, pub),
%     ok = erlzmq:bind(Publisher, "tcp://*:5571"),
%     loop(Snapshot, Publisher),
%
%
%     ok = erlzmq:term(Context).
%
% loop(Snapshot, Publisher) ->
%     perform(Snapshot),
%     perform_pub(Publisher),
%     loop(Snapshot, Publisher).
%
% perform_pub(Publisher) ->
%     %% Write two messages, each with an envelope and content
%     ok = erlzmq:send(Publisher, <<"A">>, [sndmore]),
%     io:format("We don't want to see this~n", []),
%     ok = erlzmq:send(Publisher, <<"We don't want to see this">>),
%     ok = erlzmq:send(Publisher, <<"B">>, [sndmore]),
%     ok = erlzmq:send(Publisher, <<"We would like to see this">>),
%     timer:sleep(1000).
%
%
% perform(Snapshot) ->
%     {ok, Identify} = erlzmq:recv(Snapshot),
%     case erlzmq:recv(Snapshot) of
%       {ok, ?SNAPSHOT} ->
%         % ok = erlzmq:setsockopt(Snapshot, identity, pid_to_list(self())),
%         ok = erlzmq:send(Snapshot, Identify, [sndmore]),
%         ok = erlzmq:send(Snapshot, <<"89">>),
%         ok = erlzmq:send(Snapshot, Identify, [sndmore]),
%         ok = erlzmq:send(Snapshot, ?SNAPSHOT_ACK);
%       {ok, ?HEARTBEAT} ->
%         HeartbeatAt = system_time:get_timestamp() + ?HEARTBEAT_DELAY;
%       _ ->
%         nothing
%     end,
%
%     timer:sleep(1000).
%