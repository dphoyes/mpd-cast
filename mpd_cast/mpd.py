#!/usr/bin/env python
from __future__ import annotations

from typing import Dict, Optional
import argparse
import anyio.abc
import anyio.streams.stapled
import mpdserver
import socket
import enum
import collections
import functools
import re
import logging
import threading
import queue
import copy
import urllib.parse
import mimetypes
from contextvars import ContextVar
from mpdserver import mpdclient
from mpdserver import errors as mpderrors
from mpdserver.logging import Logger
import zeroconf
import pychromecast.discovery
import pychromecast.controllers.media

InstanceName = ContextVar('instance_name')

old_log_record_factory = logging.getLogRecordFactory()
@logging.setLogRecordFactory
def record_factory(*args, **kwargs):
    record = old_log_record_factory(*args, **kwargs)
    try:
        record.instance_name = InstanceName.get() + '@'
    except LookupError:
        record.instance_name = ''
    return record


logger = Logger(__name__)
logging.basicConfig(
    format='[%(levelname)-8s %(instance_name)s%(name)s]: %(message)s',
    level=logging.INFO,
)
logging.getLogger(__name__).setLevel(logging.INFO)
logging.getLogger('mpdserver').setLevel(logging.INFO)
logging.getLogger('pychromecast.discovery').setLevel(logging.DEBUG)
logging.getLogger('pychromecast.socket_client').setLevel(logging.DEBUG)


class PlayState(enum.Enum):
    play = enum.auto()
    stop = enum.auto()
    pause = enum.auto()


class ChromecastDiscoveryListener(pychromecast.discovery.AbstractCastListener):
    def __init__(self):
        self.change_q = queue.SimpleQueue()

    def attach_browser(self, browser):
        self.browser = browser

    def add_cast(self, uuid, service):
        """Called when a new cast has beeen discovered."""
        logger.info("Discovered chromecast added: {}, {}", uuid, service)
        self.change_q.put_nowait(('+', self.browser.devices[uuid]))

    def update_cast(self, uuid, service):
        """Called when a cast has beeen updated (MDNS info renewed or changed)."""
        logger.info("Discovered chromecast updated: {}, {}", uuid, service)
        self.change_q.put_nowait(('-+', self.browser.devices[uuid]))

    def remove_cast(self, uuid, service, cast_info):
        """Called when a cast has beeen lost (MDNS info expired or host down)."""
        logger.info("Discovered chromecast removed: {}, {}", uuid, service)
        self.change_q.put_nowait(('-', cast_info))


async def discover_chromecasts(zconf):
    listener = ChromecastDiscoveryListener()
    browser = pychromecast.discovery.CastBrowser(listener, zconf)
    listener.attach_browser(browser)
    browser.start_discovery()

    try:
        while True:
            yield await anyio.run_sync_in_worker_thread(listener.change_q.get, cancellable=True)
    finally:
        browser.stop_discovery()
        listener.change_q.put_nowait(None)


class CastListener:
    def __init__(self, cast, session_id, q):
        self._cast = cast
        self._session_id = session_id
        self._q = q

    def register(self, **callback):
        k, v = callback.popitem()
        assert not callback

        cast = self._cast
        session_id = self._session_id
        q = self._q

        def new_status(status):
            if cast.status.session_id == session_id:
                q.put_nowait(functools.partial(v, copy.deepcopy(status)))
            else:
                logger.info("Ignoring message because it's not for our session: {}", status)

        setattr(self, k, new_status)

        return self


class MpdCastController(pychromecast.controllers.BaseController):
    def __init__(self, app_id, app_namespace="urn:x-cast:io.github.dphoyes"):
        super().__init__(app_namespace, app_id)
        self.listeners = []

    def register_listener(self, listener):
        self.listeners.append(listener)

    def receive_message(self, message, data):
        logger.info("Received an mpd cast message: {}", data)
        for l in self.listeners:
            l.new_mpd_cast_message(data)
        handled = True
        return handled


class Client(mpdserver.MpdClientHandler):
    @classmethod
    def define_commands(cls, register):

        class HandlerBase(mpdserver.HandlerBase):
            server: Server
            client: Client

        class CommandBase(mpdserver.CommandBase, HandlerBase): pass
        class Command(mpdserver.Command, HandlerBase): pass
        class CommandItems(mpdserver.CommandItems, HandlerBase): pass

        class ForwardedCommand(CommandBase):
            def run(self):
                return self.client.proxy_mpd.raw_command(self.raw_command, forwarding_mode=True)

        class ForwardedCommandWithStatusUpdate(ForwardedCommand):
            async def run(self):
                songid = self.server.status_from_proxy.get(b"songid")
                try:
                    async for line in super().run():
                        yield line
                finally:
                    await self.server.update_status_from_proxy()
                    if songid != self.server.status_from_proxy.get(b"songid"):
                        self.server.current_time = 0

        class ListForwardedCommand(ForwardedCommand):
            class CommandListHandler(mpdserver.CommandListBase):
                def run(self):
                    begin = b'command_list_ok_begin\n' if self.list_ok else b'command_list_begin\n'
                    end = b'command_list_end\n'
                    cmd = begin + b''.join(c.raw_command for c in self.commands) + end
                    if any(isinstance(c, ForwardedCommandWithStatusUpdate) for c in self.commands):
                        command_cls = ForwardedCommandWithStatusUpdate
                    else:
                        command_cls = ForwardedCommand
                    return command_cls(cmd, client=self.client).run()

        class ListForwardedCommandWithStatusUpdate(ForwardedCommandWithStatusUpdate, ListForwardedCommand):
            pass

        @register
        class currentsong(ListForwardedCommand): pass
        @register
        class stats(ListForwardedCommand): pass # TODO
        @register
        class consume(ListForwardedCommandWithStatusUpdate): pass
        @register
        class crossfade(ListForwardedCommandWithStatusUpdate): pass
        @register
        class mixrampdb(ListForwardedCommandWithStatusUpdate): pass
        @register
        class mixrampdelay(ListForwardedCommandWithStatusUpdate): pass
        @register
        class random(ListForwardedCommandWithStatusUpdate): pass
        @register
        class repeat(ListForwardedCommandWithStatusUpdate): pass
        @register
        class single(ListForwardedCommandWithStatusUpdate): pass
        @register
        class replay_gain_mode(ListForwardedCommandWithStatusUpdate): pass
        @register
        class replay_gain_status(ListForwardedCommand): pass
        @register
        class next(ListForwardedCommandWithStatusUpdate): pass
        @register
        class previous(ListForwardedCommandWithStatusUpdate): pass
        @register
        class add(ListForwardedCommandWithStatusUpdate): pass
        @register
        class addid(ListForwardedCommandWithStatusUpdate): pass
        @register
        class clear(ListForwardedCommandWithStatusUpdate): pass
        @register
        class delete(ListForwardedCommandWithStatusUpdate): pass
        @register
        class deleteid(ListForwardedCommandWithStatusUpdate): pass
        @register
        class move(ListForwardedCommandWithStatusUpdate): pass
        @register
        class moveid(ListForwardedCommandWithStatusUpdate): pass
        @register
        class playlist(ListForwardedCommand): pass
        @register
        class playlistfind(ListForwardedCommand): pass
        @register
        class playlistid(ListForwardedCommand): pass
        @register
        class playlistinfo(ListForwardedCommand): pass
        @register
        class playlistsearch(ListForwardedCommand): pass
        @register
        class plchanges(ListForwardedCommand): pass
        @register
        class plchangesposid(ListForwardedCommand): pass
        @register
        class prio(ListForwardedCommandWithStatusUpdate): pass
        @register
        class prioid(ListForwardedCommandWithStatusUpdate): pass
        @register
        class rangeid(ListForwardedCommandWithStatusUpdate): pass
        @register
        class shuffle(ListForwardedCommandWithStatusUpdate): pass
        @register
        class swap(ListForwardedCommandWithStatusUpdate): pass
        @register
        class swapid(ListForwardedCommandWithStatusUpdate): pass
        @register
        class addtagid(ListForwardedCommand): pass
        @register
        class cleartagid(ListForwardedCommand): pass
        @register
        class listplaylist(ListForwardedCommand): pass
        @register
        class listplaylistinfo(ListForwardedCommand): pass
        @register
        class listplaylists(ListForwardedCommand): pass
        @register
        class load(ListForwardedCommandWithStatusUpdate): pass
        @register
        class playlistadd(ListForwardedCommand): pass
        @register
        class playlistclear(ListForwardedCommand): pass
        @register
        class playlistdelete(ListForwardedCommand): pass
        @register
        class playlistmove(ListForwardedCommand): pass
        @register
        class rename(ListForwardedCommand): pass
        @register
        class rm(ListForwardedCommand): pass
        @register
        class save(ListForwardedCommand): pass
        @register
        class albumart(ListForwardedCommand): pass
        @register
        class count(ListForwardedCommand): pass
        @register
        class getfingerprint(ListForwardedCommand): pass
        @register
        class find(ListForwardedCommand): pass
        @register
        class findadd(ListForwardedCommandWithStatusUpdate): pass
        @register
        class List(ListForwardedCommand): pass
        @register
        class listall(ListForwardedCommand): pass
        @register
        class listallinfo(ListForwardedCommand): pass
        @register
        class listfiles(ListForwardedCommand): pass
        @register
        class lsinfo(ListForwardedCommand): pass
        @register
        class readcomments(ListForwardedCommand): pass
        @register
        class readpicture(ListForwardedCommand): pass
        @register
        class search(ListForwardedCommand): pass
        @register
        class searchadd(ListForwardedCommandWithStatusUpdate): pass
        @register
        class searchaddpl(ListForwardedCommand): pass
        @register
        class update(ListForwardedCommandWithStatusUpdate): pass
        @register
        class rescan(ListForwardedCommandWithStatusUpdate): pass
        @register
        class mount(ListForwardedCommand): pass
        @register
        class unmount(ListForwardedCommand): pass
        @register
        class listmounts(ListForwardedCommand): pass
        @register
        class listneighbors(ListForwardedCommand): pass
        @register
        class sticker(ListForwardedCommand): pass
        @register
        class ping(ListForwardedCommand): pass
        @register
        class tagtypes(ListForwardedCommand): pass
        @register
        class partition(ListForwardedCommand): pass
        @register
        class listpartitions(ListForwardedCommand): pass
        @register
        class newpartition(ListForwardedCommand): pass
        @register
        class delpartition(ListForwardedCommand): pass
        @register
        class moveoutput(ListForwardedCommand): pass # TODO
        @register
        class outputset(ListForwardedCommand): pass # TODO
        @register
        class config(ListForwardedCommand): pass # TODO
        @register
        class commands(ListForwardedCommand): pass # TODO
        @register
        class notcommands(ListForwardedCommand): pass # TODO
        @register
        class decoders(ListForwardedCommand): pass
        @register
        class subscribe(ListForwardedCommand): pass
        @register
        class unsubscribe(ListForwardedCommand): pass
        @register
        class channels(ListForwardedCommand): pass
        @register
        class readmessages(ListForwardedCommand): pass
        @register
        class sendmessage(ListForwardedCommand): pass

        @register
        class urlhandlers(CommandItems):
            def items(self):
                return []

        @register
        class outputs(CommandItems):
            def items(self):
                current_oid = self.server.current_output_id
                for i, output in list(self.server.outputs.items()):
                    yield 'outputid', i
                    yield 'outputname', output.friendly_name
                    yield 'outputenabled', int(i == current_oid)

        @register
        class enableoutput(Command):
            formatArg = {'oid': int}

            async def handle_args(self, oid):
                if self.server.current_output_id != oid:
                    assert oid in self.server.outputs
                    self.server.current_output_id = oid
                    await self.server.notify_idle('output')
                    await self.server.trigger_cast_state_update.set()

        @register
        class disableoutput(Command):
            formatArg = {'oid': int}

            async def handle_args(self, oid):
                if self.server.current_output_id == oid:
                    self.server.current_output_id = None
                    await self.server.notify_idle('output')
                    await self.server.trigger_cast_state_update.set()

        @register
        class toggleoutput(Command):
            formatArg = {'oid': int}

            async def handle_args(self, oid):
                if self.server.current_output_id == oid:
                    self.server.current_output_id = None
                else:
                    assert oid in self.server.outputs
                    self.server.current_output_id = oid
                await self.server.notify_idle('output')
                await self.server.trigger_cast_state_update.set()

        @register
        class status(CommandItems):
            def items(self):
                yield from list(self.server.status_from_proxy.items())
                yield b'state', self.server.play_state.name
                yield b'time', "{}:{}".format(
                    round(self.server.current_time),
                    round(float(self.server.status_from_proxy.get(b'duration', b'0').decode('utf8')))
                )
                yield b'elapsed', self.server.current_time
                try:
                    status = self.server.cast.status
                    yield b'volume', round(status.volume_level * 100) * (not status.volume_muted)
                except AttributeError:
                    yield b'volume', -1
                yield b'xfade', 0
                yield b'mixrampdb', 0
                yield b'mixrampdelay', 0

        @register
        class play(ForwardedCommandWithStatusUpdate):
            async def run(self):
                async for x in super().run():
                    yield x
                if self.server.current_output_id is not None:
                    self.server.play_state = PlayState.play
                    await self.server.notify_idle('player')
                    await self.server.trigger_cast_state_update.set()

        @register
        class playid(play):
            pass

        @register
        class pause(Command):
            formatArg = {'state': mpdserver.OptInt}

            async def handle_args(self, state=None):
                if self.server.current_output_id is not None:
                    state_before = self.server.play_state
                    if state == 1:
                        if self.server.play_state == PlayState.play:
                            self.server.play_state = PlayState.pause
                    elif state == 0:
                        if self.server.play_state == PlayState.pause:
                            self.server.play_state = PlayState.play
                    elif state is None:
                        if self.server.play_state == PlayState.play:
                            self.server.play_state = PlayState.pause
                        elif self.server.play_state == PlayState.pause:
                            self.server.play_state = PlayState.play
                    else:
                        raise mpderrors.InvalidArgumentValue("Boolean (0/1) expected", state)
                    if self.server.play_state is not state_before:
                        await self.server.notify_idle('player')
                        await self.server.trigger_cast_state_update.set()

        @register
        class stop(Command):
            async def handle_args(self):
                if self.server.play_state != PlayState.stop:
                    self.server.play_state = PlayState.stop
                    self.server.current_time = 0
                    await self.server.notify_idle('player')
                    await self.server.trigger_cast_state_update.set()

        @register
        class seek(ForwardedCommandWithStatusUpdate):
            formatArg = {"song": str, "time": float}

            async def run(self):
                async for x in super().run():
                    yield x
                self.server.current_time = self.parse_args()['time']
                await self.server.notify_idle('player')
                await self.server.trigger_cast_state_update.set()

        @register
        class seekid(seek):
            pass

        @register
        class seekcur(Command):
            formatArg = {"time": str}

            async def handle_args(self, time):
                if time[0] in ('+', '-'):
                    self.server.current_time += float(time)
                else:
                    self.server.current_time = float(time)
                await self.server.notify_idle('player')
                await self.server.trigger_cast_state_update.set()

        @register
        class setvol(Command):
            formatArg = {"vol": float}

            async def handle_args(self, vol):
                vol /= 100
                cast = self.server.cast
                if cast is not None:
                    await anyio.run_sync_in_worker_thread(lambda: cast.set_volume(vol))

        @register
        class volume(Command):
            formatArg = {"change": float}

            async def handle_args(self, change):
                change /= 100
                cast = self.server.cast
                if cast is not None:
                    await anyio.run_sync_in_worker_thread(lambda: cast.set_volume(cast.status.volume_level + change))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxy_mpd = self.server.MpdProxyClient()

    async def run(self):
        async with anyio.create_task_group() as tg:
            async def background():
                try:
                    async with self.proxy_mpd:
                        async for _ in self.proxy_mpd.idle("database"):
                            pass
                except ConnectionError:
                    logger.info("Client: Lost connection with proxy mpd")
                    await tg.cancel_scope.cancel()
            await tg.spawn(background)
            await super().run()
            await tg.cancel_scope.cancel()


class Server(mpdserver.MpdServer):
    outputs: Dict[int, pychromecast.discovery.CastInfo]
    output_id_by_uuid: Dict[str, int]
    current_output_id: Optional[int] = None
    zconf: zeroconf.Zeroconf
    cast_app_id: str
    web_host: str
    cast: Optional[pychromecast.Chromecast] = None
    cast_controller: Optional[MpdCastController] = None
    cast_session_id: Optional[str] = None
    saved_cast_media_status: Optional[pychromecast.controllers.media.MediaStatus] = None
    cast_status_thread_queue: queue.SimpleQueue
    cast_media_status_queue: anyio.streams.stapled.StapledObjectStream
    proxy_mpd: mpdclient.MpdClient
    play_state: PlayState
    __current_time_override: Optional[float]
    __new_time_change: bool
    status_from_proxy: Dict[bytes, bytes]
    trigger_cast_state_update: anyio.abc.Event

    def __init__(self, port, db, default_partition, main: MainProgram):
        super().__init__(port=port, ClientHandler=Client, Playlist=object)
        self.db = db
        self.default_partition = default_partition
        self.outputs = main.outputs
        self.output_id_by_uuid = main.output_id_by_uuid
        self.zconf = main.zconf
        self.cast_app_id = main.args.app_id
        self.web_host = main.args.web_host
        self.cast_status_thread_queue = queue.SimpleQueue()
        self.cast_media_status_queue = anyio.streams.stapled.StapledObjectStream(
            *anyio.create_memory_object_stream(100, item_type=pychromecast.controllers.media.MediaStatus)
        )
        self.proxy_mpd = self.MpdProxyClient()
        self.play_state = PlayState.stop
        self.current_time = 0
        self.status_from_proxy = {}
        self.trigger_cast_state_update = anyio.create_event()

    async def run(self):
        InstanceName.set(f"Instance{self.port}")
        async with anyio.create_task_group() as tg:
            await tg.spawn(self.__background)
            await super().run()
            await tg.cancel_scope.cancel()

    def MpdProxyClient(self):
        return mpdclient.MpdClient(**self.db, default_partition=self.default_partition)

    @property
    def current_time(self):
        if self.__current_time_override is not None:
            return self.__current_time_override
        return self.saved_cast_media_status.adjusted_current_time

    @current_time.setter
    def current_time(self, value):
        self.__current_time_override = value
        self.__new_time_change = True

    async def __background(self):
        while True:
            try:
                async with self.proxy_mpd:
                    async with anyio.create_task_group() as tg:
                        await tg.spawn(self.__forward_idle)
                        await tg.spawn(self.__handle_proxy_output)
                        await tg.spawn(self.__handle_proxy_status)
                        await tg.spawn(self.__keep_updating_cast_state)
                        await tg.spawn(self.__handle_cast_events_from_thread_queue)
                        await tg.spawn(self.__handle_cast_media_status)
            except ConnectionError:
                logger.info("Server: Lost connection with proxy mpd")
                await anyio.sleep(1)

    async def __forward_idle(self):
        async for subsystem in self.proxy_mpd.idle(
            "database", "stored_playlist", "sticker", "partition",
            "subscription", "message", "mixer",
            initial_trigger=True,
        ):
            await self.notify_idle(subsystem)

    async def __handle_proxy_output(self):
        async for subsystem in self.proxy_mpd.idle("output", initial_trigger=True):
            assert subsystem == "output", f"Got subsystem {subsystem}"
            for output in await self.proxy_mpd.command_returning_raw_objects(b"outputs", delimiter=b"outputid"):
                outputenabled = output[b"outputenabled"]
                assert outputenabled in (b"0", b"1")
                if output[b"outputenabled"] == b"1":
                    await self.proxy_mpd.command_returning_nothing(b"disableoutput " + output[b"outputid"])
                    logger.info("Disabled output of proxy mpd")

    async def __handle_proxy_status(self):
        async for changed in self.proxy_mpd.idle(
            "player", "playlist", "options", "update",
            initial_trigger=True, split=False,
        ):
            changed = frozenset(changed)
            assert changed <= {"player", "playlist", "options", "update"}, f"Got {changed}"
            status = await self.update_status_from_proxy()
            if self.play_state != PlayState.stop:
                state = status[b"state"]
                assert state in (b"stop", b"play", b"pause")
                if state == b"stop":
                    await self.proxy_mpd.command_returning_nothing(b"play")
                    logger.info("Taken proxy mpd out of 'stop' state")
            if 'player' in changed:
                await self.trigger_cast_state_update.set()
            for c in changed:
                await self.notify_idle(c)

    async def update_status_from_proxy(self):
        status = await self.proxy_mpd.command_returning_raw_object(b"status")
        self.status_from_proxy = {k: status[k] for k in (
            b"repeat",
            b"random",
            b"single",
            b"consume",
            b"song",
            b"songid",
            b"nextsong",
            b"nextsongid",
            b"duration",
            b"audio",
            b"bitrate",
            b"playlist",
            b"playlistlength",
            b"updating_db",
        ) if k in status}
        if b"songid" not in status and self.play_state != PlayState.stop:
            self.play_state = PlayState.stop
            await self.notify_idle("player")
        return status

    async def __handle_cast_events_from_thread_queue(self):
        try:
            while True:
                callback = await anyio.run_sync_in_worker_thread(self.cast_status_thread_queue.get, cancellable=True)
                await callback()
        finally:
            self.cast_status_thread_queue.put_nowait(None)

    async def __handle_cast_media_status(self):
        sent_next_for_finished_song = False
        while True:
            status: pychromecast.controllers.media.MediaStatus = await self.cast_media_status_queue.receive()
            logger.info("Received MEDIA STATUS: {}", status)
            if status.player_state == "UNKNOWN":
                prev_state = self.saved_cast_media_status
                logger.info("  player state UNKNOWN")
                if prev_state is not None and prev_state.player_state != "UNKNOWN":
                    logger.info("  previous state wasn't")
                    self.current_time = self.current_time
                    await self.trigger_cast_state_update.set()
                self.saved_cast_media_status = status
            else:
                self.saved_cast_media_status = status
                if status.player_is_playing:
                    logger.info("  player state playing")
                    sent_next_for_finished_song = False
                    self.play_state = PlayState.play
                elif status.player_is_idle:
                    logger.info("  player state idle because {}, already-sent-next={}", status.idle_reason, sent_next_for_finished_song)
                    if status.idle_reason in {'FINISHED', 'ERROR'} and not sent_next_for_finished_song:
                        sent_next_for_finished_song = True
                        await self.proxy_mpd.command_returning_nothing(b"next")
                elif status.player_is_paused:
                    logger.info("  player state paused")
                    self.play_state = PlayState.pause
                else:
                    raise AssertionError
            await self.notify_idle('player')

    async def __handle_cast_receiver_status(self, status):
        logger.info("Received RECEIVER STATUS: {}", status)
        await self.notify_idle("mixer")

    async def __handle_cast_launch_error(self, status):
        logger.info("Received LAUNCH ERROR: {}", status)

    async def __handle_cast_connection_status(self, status):
        logger.info("Received CONNECTION STATUS: {}", status)

    async def __handle_mpd_cast_message(self, message):
        logger.info("Received MPD CAST MESSAGE: {}", message)
        if message.get('type') == 'QUEUE_UPDATE':
            jump = message.get('jump')
            if jump is not None:
                if jump > 0:
                    for _ in range(jump):
                        await self.proxy_mpd.command_returning_nothing(b"next")
                elif jump < 0:
                    for _ in range(-jump):
                        await self.proxy_mpd.command_returning_nothing(b"previous")

    async def __keep_updating_cast_state(self):
        while True:
            await self.trigger_cast_state_update.wait()
            self.trigger_cast_state_update = anyio.create_event()

            while True:
                try:
                    await self.__attempt_update_cast_state()
                except pychromecast.error.PyChromecastError as e:
                    logger.warning("Retrying due to Chromecast error: {}", e)
                    await anyio.sleep(1)
                    continue
                else:
                    break

    async def __attempt_update_cast_state(self):
        # End the current session if needed
        if self.cast is not None:
            disconnected = (
                self.cast.media_controller.status.player_state == "UNKNOWN"
                or self.cast.status.session_id != self.cast_session_id
            )
            if disconnected:
                logger.info("__keep_updating_cast_state: disconnected, playstate={}", self.play_state)
            if (disconnected or self.current_output_id is None) and self.play_state == PlayState.play:
                logger.info("  pausing due to disconnection")
                self.play_state = PlayState.pause
                await self.notify_idle('player')
            if disconnected or self.current_output_id != self.output_id_by_uuid[self.cast.device.uuid]:
                logger.info("  exiting app due to disconnection")
                await anyio.run_sync_in_worker_thread(self.sync_quit_our_cast_app)
                logger.info("  app exited")
                self.cast.__del__()
                self.cast = None
                self.cast_controller = None
                self.cast_session_id = None

        # Start a session
        if self.cast is None and self.current_output_id is not None and self.play_state == PlayState.play:
            try:
                output = self.outputs[self.current_output_id]
            except KeyError:
                logger.info("Attempted to start Chromecast app, but the selected output is no longer visible")
                self.play_state = PlayState.pause
                self.current_output_id = None
                await self.notify_idle('player')
            else:
                logger.info("Starting Chromecast app")
                self.cast = pychromecast.get_chromecast_from_cast_info(output, self.zconf)

                def blocking_launch():
                    logger.info("Waiting for cast")
                    self.cast.wait()
                    controller = MpdCastController(app_id=self.cast_app_id)
                    self.cast.register_handler(controller)
                    q = queue.SimpleQueue()
                    controller.launch(lambda: q.put_nowait(None))
                    q.get()

                    listener = CastListener(self.cast, self.cast.status.session_id, self.cast_status_thread_queue)
                    self.cast.register_status_listener(listener.register(new_cast_status=self.__handle_cast_receiver_status))
                    self.cast.register_launch_error_listener(listener.register(new_launch_error=self.__handle_cast_launch_error))
                    self.cast.register_connection_listener(listener.register(new_connection_status=self.__handle_cast_connection_status))
                    self.cast.media_controller.register_status_listener(listener.register(new_media_status=self.cast_media_status_queue.send))
                    controller.register_listener(listener.register(new_mpd_cast_message=self.__handle_mpd_cast_message))

                    return controller

                self.cast_controller = await anyio.run_sync_in_worker_thread(blocking_launch)
                self.cast_session_id = self.cast.status.session_id
                logger.info("Launched Chromecast app")

        # Send state updates
        if self.cast is not None:
            async def send_media_command(command):
                media_controller = self.cast.media_controller
                media_session_id = media_controller.status.media_session_id
                if media_session_id is not None and command["type"] != "LOAD":
                    command["mediaSessionId"] = media_session_id
                def run():
                    ev = threading.Event()
                    media_controller.send_message(command, inc_session_id=True, callback_function=lambda data: ev.set())
                    ev.wait()
                logger.info("Sending chromecast command {}", command)
                await anyio.run_sync_in_worker_thread(run, cancellable=True)
                logger.info("Done sending chromecast command {}", command)

            songid = self.status_from_proxy.get(b"songid")
            if songid is None:
                assert self.play_state == PlayState.stop
            songid_currently_casting = self.cast.media_controller.status.media_custom_data.get("mpdsongid")
            if songid_currently_casting is not None:
                songid_currently_casting = songid_currently_casting.encode('utf8')

            current_time_override = self.__current_time_override
            self.__new_time_change = False

            if self.play_state == PlayState.stop:
                if self.cast.media_controller.status.player_state != "IDLE":
                    await send_media_command({"type": "STOP"})
                    assert self.cast.media_controller.status.player_state == "IDLE"
            elif songid == songid_currently_casting and not self.cast.media_controller.status.player_is_idle:
                if current_time_override is not None:
                    await send_media_command({
                        "type": "SEEK",
                        "currentTime": current_time_override,
                        "resumeState": "PLAYBACK_START" if self.play_state == PlayState.play else "PLAYBACK_PAUSE",
                    })
                elif self.play_state != PlayState.play and self.cast.media_controller.status.player_is_playing:
                    await send_media_command({"type": "PAUSE"})
                    assert self.cast.media_controller.status.player_is_paused
                elif self.play_state == PlayState.play and not self.cast.media_controller.status.player_is_playing:
                    await send_media_command({"type": "PLAY"})
                    assert self.cast.media_controller.status.player_is_playing
            else:
                assert songid is not None
                song_info = await self.proxy_mpd.command_returning_raw_object(b"playlistid " + songid)
                filepath = song_info[b'file'].decode('utf8')
                metadata = {'metadataType': 3}
                if filepath.startswith("http://"):
                    content_url = filepath
                    filepath_for_mime = filepath.split('?', maxsplit=1)[0]
                    metadata["title"] = content_url
                else:
                    quoted_filepath = urllib.parse.quote(filepath)
                    content_url = f"https://{self.web_host}/media/{quoted_filepath}"
                    filepath_for_mime = filepath
                    metadata.update({
                        'metadataType': 3,
                        "title": song_info[b'Title'].decode('utf8'),
                        'albumName': song_info[b'Album'].decode('utf8'),
                        'artist': song_info[b'Artist'].decode('utf8'),
                        'images': [{'url': f"https://{self.web_host}/albumart/{quoted_filepath}"}],
                    })
                mime, _ = mimetypes.guess_type(filepath_for_mime)
                assert mime is not None, "Couldn't guess mimetype for {}".format(filepath)
                queue_items = [{
                    "media": {
                        "contentId": content_url,
                        "contentUrl": content_url,
                        "customData": {'mpdsongid': songid.decode('utf8')},
                        "streamType": "BUFFERED",
                        "contentType": mime,
                        "metadata": metadata,
                    },
                    "orderId": 0,
                }]
                await send_media_command({
                    "type": "LOAD",
                    "queueData": {
                        "id": 1,
                        "name": "MPD Queue",
                        "items": queue_items,
                        "startIndex": 0,
                    },
                    "autoplay": self.play_state == PlayState.play,
                    "currentTime": current_time_override or 0,
                    "customData": {},
                })
                if songid is not None:
                    assert self.cast.media_controller.status.media_custom_data.get("mpdsongid").encode('utf8') == songid
            if not self.__new_time_change:
                self.__current_time_override = None

    def sync_quit_our_cast_app(self):
        if self.cast is not None and self.cast.status.session_id == self.cast_session_id:
            try:
                self.cast.quit_app()
            except pychromecast.error.NotConnected:
                pass


class MainProgram:
    args: argparse.Namespace
    local_hostname: str
    child_servers: Dict[int, Server]
    outputs: Dict[int, pychromecast.discovery.CastInfo]
    output_id_by_uuid: Dict[str, int]
    zconf: zeroconf.Zeroconf

    @staticmethod
    def main():
        main = MainProgram()
        try:
            anyio.run(main.run, backend='trio')
        finally:
            for s in main.child_servers.values():
                s.sync_quit_our_cast_app()

    def __init__(self):
        self.args = self.parse_args()
        self.local_hostname = socket.gethostname()
        self.child_servers = {}
        self.outputs = {}
        self.output_id_by_uuid = {}
        self.zconf = zeroconf.Zeroconf()

    @staticmethod
    def parse_args():
        p = argparse.ArgumentParser()
        p.add_argument('-p', '--ports', required=True)
        p.add_argument('--db', required=True)
        p.add_argument('--app-id', required=True)
        p.add_argument('--web-host', required=True)
        return p.parse_args()

    async def run(self):
        logger.debug("Starting main program")
        ports = [int(p) for p in self.args.ports.split(',')]
        db_match = re.match(r"^(?P<host>[^:]+)(:(?P<port>\d+))$", self.args.db)
        if db_match:
            db = db_match.groupdict()
        else:
            db = {"host": self.args.db}
        db["port"] = int(db.get("port", 6600))

        async with anyio.create_task_group() as tg:
            for p in ports:
                self.child_servers[p] = server = Server(
                    port=p,
                    db=db,
                    default_partition=f"mpd-cast-{self.local_hostname}-{p}",
                    main=self,
                )
                await tg.spawn(server.run)
            await tg.spawn(self.discover_chromecasts)

    async def discover_chromecasts(self):
        async for change, cast_service in discover_chromecasts(self.zconf):
            if change in {'+', '-+'}:
                try:
                    oid = self.output_id_by_uuid[cast_service.uuid]
                except KeyError:
                    oid = self.output_id_by_uuid[cast_service.uuid] = len(self.output_id_by_uuid)
                self.outputs[oid] = cast_service
                logger.info('Found chromecast {}', cast_service.friendly_name)
            elif change == '-':
                try:
                    oid = self.output_id_by_uuid[cast_service.uuid]
                    del self.outputs[oid]
                except KeyError:
                    pass
                logger.info('Lost chromecast {}', cast_service.friendly_name)
            else:
                raise AssertionError
            for s in self.child_servers.values():
                await s.notify_idle('output')


if __name__ == "__main__":
    MainProgram.main()
