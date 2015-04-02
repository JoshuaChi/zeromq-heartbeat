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
    gen_server:start_link({local, publisher}, publisher, [], [{timeout, 30000}]).

init([]) ->
    case erlzmq:context() of
      {ok, Context} ->
        {ok, State} = case erlzmq:socket(Context, [router]) of 
          {ok, Snapshot} ->
            case erlzmq:bind(Snapshot, "tcp://*:5570") of
              ok ->
                HeartbeatAt = system_time:get_timestamp() + ?HEARTBEAT_DELAY,
                {ok, accept_snapshot(#server_state{zmq_context=Context, snapshot_socket=Snapshot, heartbeat_at=HeartbeatAt})};
              _ ->
                {stop, failed_bind_zmq_5570 }
            end;
          _ ->
            {stop,  failed_create_router_socket }
        end,
        
        case erlzmq:socket(Context, [pub]) of 
          {ok, Publisher} ->
            case erlzmq:bind(Publisher, "tcp://*:5571") of
              ok ->
                {ok, accept_pub(State#server_state{publisher_socket=Publisher})};
              _ ->
                {stop,  failed_bind_zmq_5571}
            end;
          _ ->
            {stop,  failed_create_pub_socket}
        end;
            
      _ ->
        {stop,  fail_to_get_zmq_context}
    end.


accept_snapshot(State=#server_state{snapshot_socket=Snapshot}) -> 
    proc_lib:spawn(publisher, accept_snapshot_loop, [self(), Snapshot]), 
    State.  

accept_pub(State =#server_state{publisher_socket=Publisher}) ->
    proc_lib:spawn(publisher, accept_pub_loop, [self(), Publisher]),  
    State.  

accept_snapshot_loop(Server, _Snapshot) ->  
    gen_server:cast(Server, {accept_snapshot_new,self(), <<"solr">>}).

accept_pub_loop(Server, _PublisherSocket) ->  
    gen_server:cast(Server, {accept_pub_new,self()}).

handle_cast({accept_snapshot_new, _FromPid, Identify}, State=#server_state{snapshot_socket=SnapshotSocket, heartbeat_at=HeartbeatAt})->
  case erlzmq:recv(SnapshotSocket, [dontwait]) of
    {ok, ?SNAPSHOT} ->
      ok = erlzmq:send(SnapshotSocket, Identify, [sndmore]),
      ok = erlzmq:send(SnapshotSocket, <<"key">>),
      ok = erlzmq:send(SnapshotSocket, Identify, [sndmore]),
      ok = erlzmq:send(SnapshotSocket, <<"value">>),
      ok = erlzmq:send(SnapshotSocket, Identify, [sndmore]),
      ok = erlzmq:send(SnapshotSocket, ?SNAPSHOT_ACK);
    {ok, ?HEARTBEAT_CMD} ->
      io:format("p:Get client heart beat, client is still alive.~n", []);
    _ ->
      nothing
  end,
  
  Now = system_time:get_timestamp(),
  NewState = case Now > HeartbeatAt of
    true ->
      ok = erlzmq:send(SnapshotSocket, Identify, [sndmore]),
      ok = erlzmq:send(SnapshotSocket, ?HEARTBEAT_CMD),
      io:format("p:Publish heartbeat from publisher.~n",[]),
      State#server_state{heartbeat_at = (Now + ?HEARTBEAT_DELAY)};
    _ ->
      State
  end,
  
  % timer:sleep(?LOOP_SLEEP),
  _Pid = proc_lib:spawn(publisher, accept_snapshot_loop, [self(), Identify]),
  {noreply, NewState};

  
handle_cast({accept_pub_new, _FromPid},State=#server_state{publisher_socket=PublisherSocket})->
  ok = erlzmq:send(PublisherSocket, <<"A">>, [sndmore]),
  ok = erlzmq:send(PublisherSocket, <<"We don't want to see this">>),
  ok = erlzmq:send(PublisherSocket, ?ROUTER_WIO, [sndmore]),
  ok = erlzmq:send(PublisherSocket, <<"We would like to see this">>),  

  timer:sleep(?LOOP_SLEEP),
  
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
terminate(normal, _State=#server_state{zmq_context=Context}) ->
   ok = erlzmq:term(Context),
   ok.