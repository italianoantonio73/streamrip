import asyncio
import logging
import os
import random
from dataclasses import dataclass

from .. import converter
from ..client import Client, Downloadable
from ..config import Config
from ..db import Database
from ..exceptions import NonStreamableError
from ..filepath_utils import clean_filename
from ..metadata import AlbumMetadata, Covers, TrackMetadata, tag_file
from ..progress import add_title, get_progress_callback, remove_title
from .artwork import download_artwork
from .media import Media, Pending
from .semaphore import global_download_semaphore, global_retry_lock

logger = logging.getLogger("streamrip")


@dataclass(slots=True)
class Track(Media):
    meta: TrackMetadata
    downloadable: Downloadable | None
    config: Config
    folder: str
    # Is None if a cover doesn't exist for the track
    cover_path: str | None
    db: Database
    # change?
    download_path: str = ""
    is_single: bool = False
    # For refreshing download URLs on retry (Akamai CDN rejects stale signed URLs)
    client: Client | None = None
    track_id: str = ""
    quality: int = 0

    async def preprocess(self):
        self._set_download_path()
        os.makedirs(self.folder, exist_ok=True)
        if self.is_single:
            add_title(self.meta.title)

    async def download(self):
        max_retries = 5
        last_error = None

        for attempt in range(max_retries):
            if attempt > 0:
                # Serialize retries across all tracks to avoid concurrent retry storms
                async with global_retry_lock():
                    base_delay = min(2**attempt, 64)
                    delay = base_delay + random.uniform(0, base_delay * 0.5)
                    logger.warning(
                        f"Error downloading track '{self.meta.title}' "
                        f"(attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {delay:.0f}s: {last_error}",
                    )
                    await asyncio.sleep(delay)

            # Fetch or refresh download URL
            if self.downloadable is None or attempt > 0:
                if self.client is not None and self.track_id:
                    try:
                        self.downloadable = await self.client.get_downloadable(
                            self.track_id, self.quality
                        )
                    except NonStreamableError:
                        raise
                    except Exception as e:
                        last_error = e
                        continue

            async with global_download_semaphore(self.config.session.downloads):
                desc = f"Track {self.meta.tracknumber}"
                if attempt > 0:
                    desc += f" (retry {attempt})"

                try:
                    with get_progress_callback(
                        self.config.session.cli.progress_bars,
                        await self.downloadable.size(),
                        desc,
                    ) as callback:
                        await self.downloadable.download(
                            self.download_path, callback
                        )
                        return set()
                except Exception as e:
                    last_error = e
                    if os.path.exists(self.download_path):
                        try:
                            os.remove(self.download_path)
                        except OSError:
                            pass

        # Try downloading an alternative version (e.g. single) with the same ISRC.
        # Some Qobuz album tracks have broken CDN entries, but the same recording
        # released as a single has a working CDN path.
        alt_id = await self._find_alternative_track()
        if alt_id is not None:
            try:
                self.downloadable = await self.client.get_downloadable(
                    alt_id, self.quality,
                )
                async with global_download_semaphore(self.config.session.downloads):
                    with get_progress_callback(
                        self.config.session.cli.progress_bars,
                        await self.downloadable.size(),
                        f"Track {self.meta.tracknumber} (alt {alt_id})",
                    ) as callback:
                        await self.downloadable.download(
                            self.download_path, callback,
                        )
                        logger.info(
                            f"Downloaded '{self.meta.title}' via alternative "
                            f"track {alt_id} (ISRC: {self.meta.isrc})",
                        )
                        return set()
            except Exception as e:
                logger.warning(f"Alternative track {alt_id} also failed: {e}")
                if os.path.exists(self.download_path):
                    try:
                        os.remove(self.download_path)
                    except OSError:
                        pass

        logger.error(
            f"Persistent error downloading track "
            f"'{self.meta.title}', skipping: {last_error}",
        )
        self.db.set_failed(
            self.downloadable.source, "track", self.meta.info.id,
        )
        return {self.meta.info.id}

    async def _find_alternative_track(self) -> str | None:
        """Search for an alternative version of a failed track by ISRC.

        When album tracks have broken CDN entries, the same recording
        released as a single often has a working CDN path.  Both share
        the same ISRC (International Standard Recording Code).

        A candidate must match all of the following criteria:
        - Same ISRC as the original track
        - Different track ID (not the same broken entry)
        - Streamable
        - Same parental_warning (explicit) flag
        - Bit depth >= the original track's bit depth
        - Sampling rate >= the original track's sampling rate
        """
        if self.client is None or self.client.source != "qobuz":
            logger.info(f"Skipping alternative search for '{self.meta.title}': no client or not qobuz")
            return None
        if not self.meta.isrc:
            logger.info(f"Skipping alternative search for '{self.meta.title}': no ISRC")
            return None

        try:
            query = f"{self.meta.title} {self.meta.artist}"
            logger.info(
                f"Searching for alternative track: query='{query}', "
                f"ISRC={self.meta.isrc}, explicit={self.meta.info.explicit}"
            )
            pages = await self.client.search("track", query, limit=20)
            candidates = 0
            for page in pages:
                items = page.get("tracks", {}).get("items", [])
                for item in items:
                    item_id = str(item["id"])
                    item_isrc = item.get("isrc")
                    item_streamable = item.get("streamable", False)
                    item_explicit = item.get("parental_warning", False)
                    item_bit_depth = item.get("maximum_bit_depth")
                    item_sampling_rate = item.get("maximum_sampling_rate")
                    if item_isrc == self.meta.isrc and item_id != self.meta.info.id:
                        candidates += 1
                        if not item_streamable:
                            logger.info(
                                f"Alternative candidate {item_id} rejected: not streamable"
                            )
                        elif item_explicit != self.meta.info.explicit:
                            logger.info(
                                f"Alternative candidate {item_id} rejected: "
                                f"explicit mismatch (track={self.meta.info.explicit}, "
                                f"candidate={item_explicit})"
                            )
                        elif (
                            self.meta.info.bit_depth is not None
                            and item_bit_depth is not None
                            and item_bit_depth < self.meta.info.bit_depth
                        ):
                            logger.info(
                                f"Alternative candidate {item_id} rejected: "
                                f"bit depth {item_bit_depth} < {self.meta.info.bit_depth}"
                            )
                        elif (
                            self.meta.info.sampling_rate is not None
                            and item_sampling_rate is not None
                            and item_sampling_rate < self.meta.info.sampling_rate
                        ):
                            logger.info(
                                f"Alternative candidate {item_id} rejected: "
                                f"sampling rate {item_sampling_rate} < {self.meta.info.sampling_rate}"
                            )
                        else:
                            logger.info(
                                f"Found alternative track {item_id} for "
                                f"'{self.meta.title}' (ISRC: {self.meta.isrc}, "
                                f"bit_depth={item_bit_depth}, "
                                f"sampling_rate={item_sampling_rate})",
                            )
                            return item_id
            logger.info(
                f"No alternative found for '{self.meta.title}' "
                f"(ISRC: {self.meta.isrc}, {candidates} ISRC match(es) rejected)"
            )
        except Exception as e:
            logger.warning(f"Alternative track search failed: {e}")
        return None

    async def postprocess(self):
        if self.is_single:
            remove_title(self.meta.title)

        if not os.path.exists(self.download_path):
            return

        await tag_file(self.download_path, self.meta, self.cover_path)
        if self.config.session.conversion.enabled:
            await self._convert()

        self.db.set_downloaded(self.meta.info.id)

    async def _convert(self):
        c = self.config.session.conversion
        engine_class = converter.get(c.codec)
        engine = engine_class(
            filename=self.download_path,
            sampling_rate=c.sampling_rate,
            bit_depth=c.bit_depth,
            remove_source=True,  # always going to delete the old file
        )
        await engine.convert()
        self.download_path = engine.final_fn  # because the extension changed

    def _set_download_path(self):
        c = self.config.session.filepaths
        formatter = c.track_format
        track_path = clean_filename(
            self.meta.format_track_path(formatter),
            restrict=c.restrict_characters,
        )
        if c.truncate_to > 0 and len(track_path) > c.truncate_to:
            track_path = track_path[: c.truncate_to]

        self.download_path = os.path.join(
            self.folder,
            f"{track_path}.{self.downloadable.extension}",
        )


@dataclass(slots=True)
class PendingTrack(Pending):
    id: str
    album: AlbumMetadata
    client: Client
    config: Config
    folder: str
    db: Database
    # cover_path is None <==> Artwork for this track doesn't exist in API
    cover_path: str | None

    async def resolve(self) -> Track | None:
        if self.db.downloaded(self.id):
            logger.info(
                f"Skipping track {self.id}. Marked as downloaded in the database.",
            )
            return None

        source = self.client.source
        try:
            resp = await self.client.get_metadata(self.id, "track")
        except NonStreamableError as e:
            logger.error(f"Track {self.id} not available for stream on {source}: {e}")
            return None

        try:
            meta = TrackMetadata.from_resp(self.album, source, resp)
        except Exception as e:
            logger.error(f"Error building track metadata for {self.id}: {e}")
            return None

        if meta is None:
            logger.error(f"Track {self.id} not available for stream on {source}")
            self.db.set_failed(source, "track", self.id)
            return None

        quality = self.config.session.get_source(source).quality
        downloadable = None
        try:
            downloadable = await self.client.get_downloadable(self.id, quality)
        except NonStreamableError as e:
            logger.error(
                f"Error getting downloadable data for track {meta.tracknumber} [{self.id}]: {e}"
            )
            return None
        except Exception as e:
            logger.warning(
                f"Transient error getting download URL for track "
                f"{meta.tracknumber} [{self.id}], will retry during download: {e}"
            )

        downloads_config = self.config.session.downloads
        if downloads_config.disc_subdirectories and self.album.disctotal > 1:
            folder = os.path.join(self.folder, f"Disc {meta.discnumber}")
        else:
            folder = self.folder

        return Track(
            meta,
            downloadable,
            self.config,
            folder,
            self.cover_path,
            self.db,
            client=self.client,
            track_id=self.id,
            quality=quality,
        )


@dataclass(slots=True)
class PendingSingle(Pending):
    """Whereas PendingTrack is used in the context of an album, where the album metadata
    and cover have been resolved, PendingSingle is used when a single track is downloaded.

    This resolves the Album metadata and downloads the cover to pass to the Track class.
    """

    id: str
    client: Client
    config: Config
    db: Database

    async def resolve(self) -> Track | None:
        if self.db.downloaded(self.id):
            logger.info(
                f"Skipping track {self.id}. Marked as downloaded in the database.",
            )
            return None

        try:
            resp = await self.client.get_metadata(self.id, "track")
        except NonStreamableError as e:
            logger.error(f"Error fetching track {self.id}: {e}")
            return None
        # Patch for soundcloud
        try:
            album = AlbumMetadata.from_track_resp(resp, self.client.source)
        except Exception as e:
            logger.error(f"Error building album metadata for track {id=}: {e}")
            return None

        if album is None:
            self.db.set_failed(self.client.source, "track", self.id)
            logger.error(
                f"Cannot stream track (am) ({self.id}) on {self.client.source}",
            )
            return None

        try:
            meta = TrackMetadata.from_resp(album, self.client.source, resp)
        except Exception as e:
            logger.error(f"Error building track metadata for track {id=}: {e}")
            return None

        if meta is None:
            self.db.set_failed(self.client.source, "track", self.id)
            logger.error(
                f"Cannot stream track (tm) ({self.id}) on {self.client.source}",
            )
            return None

        config = self.config.session
        quality = getattr(config, self.client.source).quality
        assert isinstance(quality, int)
        parent = config.downloads.folder
        if config.filepaths.add_singles_to_folder:
            folder = self._format_folder(album)
        else:
            folder = parent

        os.makedirs(folder, exist_ok=True)

        embedded_cover_path, downloadable = await asyncio.gather(
            self._download_cover(album.covers, folder),
            self.client.get_downloadable(self.id, quality),
        )
        return Track(
            meta,
            downloadable,
            self.config,
            folder,
            embedded_cover_path,
            self.db,
            is_single=True,
            client=self.client,
            track_id=self.id,
            quality=quality,
        )

    def _format_folder(self, meta: AlbumMetadata) -> str:
        c = self.config.session
        parent = c.downloads.folder
        formatter = c.filepaths.folder_format
        if c.downloads.source_subdirectories:
            parent = os.path.join(parent, self.client.source.capitalize())

        return os.path.join(parent, meta.format_folder_path(formatter))

    async def _download_cover(self, covers: Covers, folder: str) -> str | None:
        embed_path, _ = await download_artwork(
            self.client.session,
            folder,
            covers,
            self.config.session.artwork,
            for_playlist=False,
        )
        return embed_path
