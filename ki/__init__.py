"""
Ki is a command-line interface for the version control and editing of `.anki2`
collections as git repositories of markdown files.  Rather than providing an
interactive UI like the Anki desktop client, ki aims to allow natural editing
*in the filesystem*.

In general, the purpose of ki is to allow users to work on large, complex Anki
decks in exactly the same way they work on large, complex software projects.
.. include:: ./DOCUMENTATION.md
"""

# pylint: disable=invalid-name, missing-class-docstring, broad-except
# pylint: disable=too-many-return-statements, too-many-lines, too-many-arguments
# pylint: disable=no-value-for-parameter, not-callable, unnecessary-lambda-assignment

import os
import re
import gc
import sys
import time
import json
import copy
import random
import logging
import sqlite3
import hashlib
import datetime
import itertools
import subprocess
import configparser
from pathlib import Path
from itertools import chain, starmap, tee
from functools import reduce
from collections import namedtuple
import platform

import git
import click
from lark import Lark

# Required to avoid circular imports because the Anki pylib codebase is gross.
import anki.collection
from anki.cards import Card
from anki.utils import ids2str
from anki.models import NotetypeDict
from anki.errors import NotFoundError
from anki.collection import Collection, Note, OpChangesWithId
from anki.importing.noteimp import NoteImporter

from beartype import beartype
from beartype.typing import (
    Set,
    List,
    Dict,
    Any,
    Optional,
    Callable,
    Union,
    TypeVar,
    Tuple,
    Iterator,
    Iterable,
    FrozenSet,
)

import ki.maybes as M
import ki.functional as F
from ki.types import (
    MODELS_FILE,
    File,
    Dir,
    EmptyDir,
    NoPath,
    NoFile,
    GitChangeType,
    Delta,
    KiRepo,
    Notetype,
    ColNote,
    KiRev,
    Rev,
    Deck,
    Root,
    DotKi,
    CardFile,
    NoteDBRow,
    DeckNote,
    NoteMetadata,
    PushResult,
    PlannedLink,
    MediaBytes,
    AddedMedia,
    UpdatesRejectedError,
    TargetExistsError,
    CollectionChecksumError,
    MissingNotetypeError,
    NotetypeMismatchError,
    NoteFieldValidationWarning,
    DeletedFileNotFoundWarning,
    DiffTargetFileNotFoundWarning,
    NotetypeCollisionWarning,
    SQLiteLockError,
    MissingMediaDirectoryError,
    WrongFieldCountWarning,
    InconsistentFieldNamesWarning,
    AnkiDBNoteMissingFieldsError,
    RenamedMediaFileWarning,
    NonEmptyWorkingTreeError,
    EmptyNoteWarning,
    DuplicateNoteWarning,
    UnhealthyNoteWarning,
    MediaDirectoryDeckNameCollisionWarning,
    notetype_json,
)
from ki.maybes import (
    GIT,
    GITIGNORE_FILE,
    GITMODULES_FILE,
    KI,
    HASHES_FILE,
    BACKUPS_DIR,
)
from ki.transformer import NoteTransformer, FlatNote

curried = F.curried

logging.basicConfig(level=logging.INFO)

TQ = F.progressbar

T = TypeVar("T")
NoteId, DeckId, CardId = int, int, int
CardFileMap = Dict[DeckId, List[CardFile]]

GITATTRS_FILE = ".gitattributes"

UTF8 = "UTF-8"
URLS = "(https?|ftp)://"
MEDIA = M.MEDIA
DEV_NULL = "/dev/null"
BATCH_SIZE = 300
HTML_REGEX = r"</?\s*[a-z-][^>]*\s*>|(\&(?:[\w\d]+|#\d+|#x[a-f\d]+);)"
REMOTE_NAME = "anki"
BRANCH_NAME = F.BRANCH_NAME
MAX_FILENAME_LEN = 60
IGNORE_DIRS = set([GIT, KI, MEDIA])
IGNORE_FILES = set([GITIGNORE_FILE, GITMODULES_FILE, MODELS_FILE])
HEAD_SUFFIX = Path("ki-head")
LOCAL_SUFFIX = Path("ki-local")
REMOTE_SUFFIX = Path("ki-remote")
FIELD_HTML_SUFFIX = Path("ki-fieldhtml")
LCA = "last-successful-ki-push"

MEDIA_FILE_RECURSIVE_PATTERN = f"**/{MEDIA}/*"

# This is the key for media files associated with notetypes instead of the
# contents of a specific note.
NOTETYPE_NID = -57

MD = ".md"

ALHPANUMERICS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
SYMBOLS = "!#$%&()*+,-./:;<=>?@[]^_`{|}~"
BASE91_TABLE = list(ALHPANUMERICS + SYMBOLS)

ADDED = GitChangeType.ADDED
RENAMED = GitChangeType.RENAMED
DELETED = GitChangeType.DELETED
MODIFIED = GitChangeType.MODIFIED
TYPECHANGED = GitChangeType.TYPECHANGED


@beartype
def do(f: Callable[[Any], Any], xs: Iterable[Any]) -> None:
    """Perform some action on an iterable."""
    list(map(f, xs))


@beartype
def stardo(f: Callable[[Any], Any], xs: Iterable[Any]) -> None:
    """Perform some action on an iterable of tuples, unpacking arguments."""
    list(starmap(f, xs))


@beartype
def lock(col_file: File) -> sqlite3.Connection:
    """Check that lock can be acquired on a SQLite3 database given a path."""
    try:
        con = sqlite3.connect(col_file, timeout=0.1)
        con.isolation_level = "EXCLUSIVE"
        con.execute("BEGIN EXCLUSIVE")
    except sqlite3.DatabaseError as err:
        raise SQLiteLockError(col_file, err) from err
    if sys.platform == "win32":
        con.commit()
        con.close()
    return con


@beartype
def unlock(con: sqlite3.Connection) -> None:
    """Unlock a SQLite3 database."""
    if sys.platform == "win32":
        return
    con.commit()
    con.close()


@beartype
def cp_repo(rev: Rev, suffix: str) -> git.Repo:
    """Get a temporary copy of a git repository in /tmp/<suffix>/."""
    # Copy the entire repo into a temp directory ending in `../suffix/`.
    target: NoFile = F.chk(F.mkdtemp() / suffix)
    ephem = git.Repo(F.copytree(F.root(rev.repo), target))

    # Do a reset --hard to the given SHA.
    ephem.git.reset(rev.sha, hard=True)
    return ephem


@beartype
def cp_ki(ki_rev: KiRev, suffix: str) -> KiRepo:
    """
    Given a KiRev, i.e. a pair of the form (kirepo, SHA), we clone
    `kirepo.repo` into a temp directory and hard reset to the given commit
    hash. Copies the .ki/ directory from `ki_rev.kirepo` without making any
    changes.

    Parameters
    ----------
    ki_rev : KiRev
        The ki repository to clone, and a commit for it.
    suffix : str
        /tmp/.../ path suffix, e.g. `ki/local/`.

    Returns
    -------
    KiRepo
        The copied ki repository.
    """
    rev: Rev = F.ki_rev_to_rev(ki_rev)
    print(F.root(rev.repo))
    ephem: git.Repo = cp_repo(rev, suffix)
    F.force_mkdir(F.root(ephem) / KI / BACKUPS_DIR)
    kirepo: KiRepo = M.kirepo(F.root(ephem))
    return kirepo


@beartype
def is_anki_note(path: File) -> bool:
    """Check if file is a `ki`-style markdown note."""
    # Ought to have markdown file extension.
    if path.suffix != ".md":
        return False
    with open(path, "r", encoding=UTF8) as md_f:
        lines = md_f.readlines()
    if len(lines) < 8:
        return False
    if lines[0] != "# Note\n":
        return False
    if lines[1] != "```\n":
        return False
    if not re.match(r"^guid: ", lines[2]):
        return False
    return True


@beartype
def is_ignorable(root: Dir, path: Path) -> bool:
    """
    Filter out paths in a git repository diff that do not correspond to Anki
    notes.

    We could do this purely using calls to `is_anki_note()`, but these are
    expensive, so we try to find matches without opening any files first.
    """
    # Ignore if `path` is an exact match for any of the patterns Since the
    # contents of a git repository diff are always going to be files, this
    # alone will not correctly ignore directory names given in `patterns`.
    #
    # If any of the patterns in `dirnames` resolve to one of the parents of
    # `path`, return a warning, so that we are able to filter out entire
    # directories.
    filenames, dirnames = IGNORE_FILES, IGNORE_DIRS
    if path.name in filenames | dirnames or len(set(path.parts) & dirnames) > 0:
        return True

    # If `path` is an extant file (not a directory) and *not* a note, ignore it.
    file = F.chk(root / path)
    if isinstance(file, File) and not is_anki_note(file):
        return True
    return False


@curried
@beartype
def mungediff(
    parse: Callable[[Delta], DeckNote], a_root: Dir, b_root: Dir, d: git.Diff
) -> Iterable[Union[Delta, Warning]]:
    """Extract deltas and warnings from a collection of diffs."""
    a, b = d.a_path, d.b_path
    a, b = a if a else b, b if b else a
    if is_ignorable(a_root, Path(a)) or is_ignorable(b_root, Path(b)):
        return []

    # Get absolute and relative paths to 'a' and 'b'.
    AB = namedtuple("AB", "a b")
    files = AB(F.chk(a_root / a), F.chk(b_root / b))
    rels = AB(Path(a), Path(b))

    if d.change_type == DELETED.value:
        if not F.isfile(files.a):
            return [DeletedFileNotFoundWarning(rels.a)]
        return [Delta(GitChangeType.DELETED, files.a, rels.a)]
    if not F.isfile(files.b):
        return [DiffTargetFileNotFoundWarning(rels.b)]
    if d.change_type == RENAMED.value:
        a_delta = Delta(GitChangeType.DELETED, files.a, rels.a)
        b_delta = Delta(GitChangeType.ADDED, files.b, rels.b)
        a_decknote, b_decknote = parse(a_delta), parse(b_delta)
        if a_decknote.guid != b_decknote.guid:
            return [a_delta, b_delta]
    return [Delta(GitChangeType(d.change_type), files.b, rels.b)]


@beartype
def diff2(
    repo: git.Repo,
    parse: Callable[[Delta], DeckNote],
) -> Iterable[Union[Delta, Warning]]:
    """Diff `repo` from `HEAD~1` to `HEAD`."""
    # We diff from A~B.
    head1: Rev = M.rev(repo, repo.commit("HEAD~1").hexsha)
    uuid = hex(random.randrange(16**4))[2:]
    head1_repo = cp_repo(head1, suffix=f"HEAD~1-{uuid}")
    a_root, b_root = F.root(head1_repo), F.root(repo)
    diffidx = repo.commit("HEAD~1").diff(repo.commit("HEAD"))

    # Get the diffs for each change type (e.g. 'DELETED').
    return chain(*map(mungediff(parse, a_root, b_root), diffidx))


@beartype
def get_models_recursively(kirepo: KiRepo) -> Dict[str, Notetype]:
    """
    Find and merge all `models.json` files recursively. Returns a dictionary
    sending model names to Notetypes.
    """

    @beartype
    def load(file: File) -> Iterable[Notetype]:
        """Load a models file."""
        with open(file, "r", encoding=UTF8) as f:
            return map(M.notetype, json.load(f).values())

    notetypes = F.cat(map(load, F.rglob(kirepo.root, MODELS_FILE)))
    return {notetype.name: notetype for notetype in notetypes}


@beartype
def check_fields_health(note: Note) -> List[Warning]:
    """Construct warnings when Anki's fields health check fails."""
    health = note.fields_check()
    if health == 1:
        return [EmptyNoteWarning(note, health)]
    if health == 2:
        return [DuplicateNoteWarning(note, health, html_to_screen(note.fields[0]))]
    if health != 0:
        return [UnhealthyNoteWarning(note, health)]
    return []


@beartype
def get_guid(fields: List[str]) -> str:
    """Construct a new GUID for a note. Adapted from genanki's `guid_for()`."""
    # Get the first 8 bytes of the SHA256 of `contents` as an int.
    m = hashlib.sha256()
    m.update("__".join(fields).encode("utf-8"))
    x = reduce(lambda h, b: (h << 8) + b, m.digest()[:8], 0)

    # convert to the weird base91 format that Anki uses
    chars = []
    while x > 0:
        chars.append(BASE91_TABLE[x % len(BASE91_TABLE)])
        x //= len(BASE91_TABLE)
    return "".join(reversed(chars))


@curried
@beartype
def parse_note(parser: Lark, transformer: NoteTransformer, delta: Delta) -> DeckNote:
    """Parse with lark."""
    tree = parser.parse(delta.path.read_text(encoding=UTF8))
    flatnote: FlatNote = transformer.transform(tree)
    parts: Tuple[str, ...] = delta.relpath.parent.parts
    deck: str = "::".join(parts)

    # Generate a GUID from the hash of the field contents if the `guid` field
    # in the note file was left blank.
    fields = list(flatnote.fields.values())
    guid = flatnote.guid if flatnote.guid != "" else get_guid(fields)

    return DeckNote(
        title=flatnote.title,
        guid=guid,
        deck=deck,
        model=flatnote.model,
        tags=flatnote.tags,
        fields=flatnote.fields,
    )


@beartype
def plain_to_html(plain: str) -> str:
    """Convert plain text to html"""
    # Minor clean up
    plain = plain.replace(r"&lt;", "<")
    plain = plain.replace(r"&gt;", ">")
    plain = plain.replace(r"&amp;", "&")
    plain = plain.replace(r"&nbsp;", " ")
    plain = re.sub(r"\<b\>\s*\<\/b\>", "", plain)
    plain = re.sub(r"\<i\>\s*\<\/i\>", "", plain)
    plain = re.sub(r"\<div\>\s*\<\/div\>", "", plain)

    # Convert newlines to `<br>` tags.
    if not re.search(HTML_REGEX, plain):
        plain = plain.replace("\n", "<br>")

    return plain.strip()


@curried
@beartype
def update_field(decknote: DeckNote, note: Note, key: str, field: str) -> None:
    """Update a field contained in `note`."""
    try:
        note[key] = plain_to_html(field)
    except IndexError as err:
        raise AnkiDBNoteMissingFieldsError(decknote, note.id, key) from err


@beartype
def update_note(
    note: Note, decknote: DeckNote, old_notetype: Notetype, new_notetype: Notetype
) -> Iterable[Warning]:
    """
    Change all the data of `note` to that given in `decknote`.

    This is only to be called on notes whose nid already exists in the
    database.  Creates a new deck if `decknote.deck` doesn't exist.  Assumes
    that the model has already been added to the collection, and raises an
    exception if it finds otherwise.  Changes notetype to that specified by
    `decknote.model`.  Overwrites all fields with `decknote.fields`.

    Updates:
    - tags
    - deck
    - model
    - fields
    """

    # Check that the passed argument `new_notetype` has a name consistent with
    # the model specified in `decknote`. The former should be derived from the
    # latter, and if they don't match, there is a bug in the caller.
    if decknote.model != new_notetype.name:
        raise NotetypeMismatchError(decknote, new_notetype)

    nid = note.id
    note.tags = decknote.tags
    note.flush()

    # Set the deck of the given note, as well as all its cards, and create a
    # deck with this name if it doesn't already exist. See the
    # comments/docstrings in the implementation of the
    # `anki.decks.DeckManager.id()` method.
    newdid: int = note.col.decks.id(decknote.deck, create=True)
    cids = [c.id for c in note.cards()]
    if cids:
        note.col.set_deck(cids, newdid)

    # Set notetype (also clears all fields).
    if old_notetype.id != new_notetype.id:
        fmap = {field.ord: None for field in old_notetype.flds}
        note.col.models.change(old_notetype.dict, [nid], new_notetype.dict, fmap, None)
        note.load()

    # Validate field keys against notetype.
    warnings: List[Warning] = validate_decknote_fields(new_notetype, decknote)
    if len(warnings) > 0:
        return warnings

    # Set field values and flush to collection database. This is correct
    # because every field name that appears in `new_notetype` is contained in
    # `decknote.fields`, or else we would have printed a warning and returned
    # above.
    missing = {key for key in decknote.fields if key not in note}
    warnings = map(lambda k: NoteFieldValidationWarning(nid, k, new_notetype), missing)
    fields = [(key, field) for key, field in decknote.fields.items() if key in note]
    stardo(update_field(decknote, note), fields)
    note.flush()

    # Remove if unhealthy.
    fwarns: List[Warning] = check_fields_health(note)
    if len(fwarns) > 0:
        note.col.remove_notes([nid])
    return chain(warnings, fwarns)


@beartype
def validate_decknote_fields(notetype: Notetype, decknote: DeckNote) -> List[Warning]:
    """Validate that the fields given in the note match the notetype."""
    warnings: List[Warning] = []
    names: List[str] = [field.name for field in notetype.flds]

    # TODO: It might also be nice to print the path of the note in the
    # repository. This would have to be added to the `DeckNote` spec.
    if len(decknote.fields.keys()) != len(names):
        warnings.append(WrongFieldCountWarning(decknote, names))

    mk_warning = lambda n, k: InconsistentFieldNamesWarning(n, k, decknote)
    names_and_keys = F.starfilter(
        lambda n, k: n != k, zip(names, decknote.fields.keys())
    )
    return warnings + list(starmap(mk_warning, names_and_keys))


@beartype
def get_note_path(colnote: ColNote, deck_dir: Dir, card_name: str = "") -> NoFile:
    """Get note path from sort field text."""
    field_text = colnote.sfld

    # Construct filename, stripping HTML tags and sanitizing (quickly).
    field_text = plain_to_html(field_text)
    field_text = re.sub("<[^<]+?>", "", field_text)

    # If the HTML stripping removed all text, we just slugify the raw sort
    # field text.
    if len(field_text) == 0:
        field_text = colnote.sfld

    name = field_text[:MAX_FILENAME_LEN]
    slug = F.slugify(name)

    # If the slug is still empty, use all the fields.
    if len(slug) == 0:
        contents = " ".join(colnote.n.values())
        name = contents[:MAX_FILENAME_LEN]
        slug = F.slugify(name)

    # Make it so `slug` cannot possibly be an empty string, because then we get
    # a `Path('.')` which is a bug, and causes a runtime exception. If all else
    # fails, use the notetype name, hash of the payload, and creation date.
    if len(slug) == 0:
        guidhex = colnote.n.guid.encode(UTF8).hex()
        slug: str = f"{colnote.notetype.name}--{guidhex}"

        # Note IDs are in milliseconds.
        dt = datetime.datetime.fromtimestamp(colnote.n.id / 1000.0)
        slug += "--" + dt.strftime("%Y-%m-%d--%Hh-%Mm-%Ss")
        F.yellow(f"Slug for note with guid '{colnote.n.guid}' is empty...")
        F.yellow(f"Using hex representation of guid in filename: '{slug}'")

    if card_name != "":
        slug = f"{slug}_{card_name}"
    filename: str = f"{slug}{MD}"
    note_path = F.chk(deck_dir / filename, resolve=False)

    i = 1
    while not isinstance(note_path, NoFile):
        filename = f"{slug}_{i}{MD}"
        note_path = F.chk(deck_dir / filename, resolve=False)
        i += 1

    return note_path


@beartype
def backup(kirepo: KiRepo) -> int:
    """Backup collection to `.ki/backups`."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d--%Hh-%Mm-%Ss")
    md5sum = F.md5(kirepo.col_file)
    name = f"{timestamp}--{md5sum}.anki2"
    backup_file = F.chk(kirepo.backups_dir / name)

    # We assume here that no one would ever make e.g. a directory called
    # `name`, since `name` contains the md5sum of the collection file, and
    # thus that is extraordinarily improbable. So the only thing we have to
    # check for is that we haven't already written a backup file to this
    # location.
    if isinstance(backup_file, File):
        return 1

    F.copyfile(kirepo.col_file, F.chk(kirepo.backups_dir / name))
    return 0


@beartype
def append_md5sum(dotki: Dir, tag: str, md5sum: str) -> None:
    """Append an md5sum hash to the hashes file."""
    hashes_file = dotki / HASHES_FILE
    with open(hashes_file, "a+", encoding=UTF8) as hashes_f:
        hashes_f.write(f"{md5sum}  {tag}\n")


@beartype
def get_field_note_id(nid: int, fieldname: str) -> str:
    """A str ID that uniquely identifies field-note pairs."""
    return f"{nid}{F.slugify(fieldname)}"


@beartype
def add_db_note(
    col: Collection,
    nid: int,
    guid: str,
    mid: int,
    mod: int,
    usn: int,
    tags: List[str],
    fields: List[str],
    sfld: str,
    csum: int,
    flags: int,
    data: str,
) -> Note:
    """Add a note to the database directly, with a SQL INSERT."""
    importer = NoteImporter(col, "")
    importer.addNew(
        [
            (
                nid,
                guid,
                mid,
                mod,
                usn,
                " " + " ".join(tags) + " ",
                "\x1f".join(fields),
                sfld,
                csum,
                flags,
                data,
            )
        ]
    )

    # All the `mark_modified` flag does is update `mod`. Since we always set
    # `mod` to the current timestamp anyway, this doesn't matter, so may as
    # well set it to `True` to reflect the semantics of the operation we're
    # performing. This may present issues down the road since newly imported
    # cards from cloned submodules will be marked modified on import/push,
    # which is not exactly right. The anki2 importer does *not* mark as
    # modified, because importing a new note does not modify its content. We
    # would need to have `mod` data inside the note grammar in order for this
    # to make sense, which may be more trouble than it's worth. Users writing
    # new notes as markdown files would have to set the `mod` to some default
    # value, or leave it blank. Assuming people don't do this nearly as often
    # as they will export or push notes they've created in Anki, then it might
    # make sense.
    col.after_note_updates([nid], mark_modified=True)
    return col.get_note(nid)


@curried
@beartype
def push_note(
    col: Collection,
    timestamp_ns: int,
    guids: Dict[str, NoteMetadata],
    new_nids: Iterator[int],
    decknote: DeckNote,
) -> Iterable[Warning]:
    """
    Update the Anki `Note` object in `col` corresponding to `decknote`,
    creating it if it does not already exist.

    Raises
    ------
    MissingNotetypeError
        If we can't find a notetype with the name provided in `decknote`.
    """
    # Notetype/model names are privileged in Anki, so if we don't find the
    # right name, we raise an error.
    model_id: Optional[int] = col.models.id_for_name(decknote.model)
    if model_id is None:
        raise MissingNotetypeError(decknote.model)
    new_notetype: Notetype = M.notetype(col.models.get(model_id))

    if decknote.guid in guids:
        nid: int = guids[decknote.guid].nid
        try:
            note: Note = col.get_note(nid)
        except NotFoundError as err:
            print(f"{nid = }")
            print(f"{decknote.guid = }")
            raise err
    else:
        nid: int = next(new_nids)
        note: Note = add_db_note(
            col,
            nid,
            decknote.guid,
            model_id,
            mod=int(timestamp_ns // 1e9),
            usn=-1,
            tags=decknote.tags,
            fields=list(decknote.fields.values()),
            sfld=decknote.fields[new_notetype.sortf.name],
            csum=0,
            flags=0,
            data="",
        )

    # If we are updating an existing note, we need to know the old and new
    # notetypes, and then update the notetype (and the rest of the note data)
    # accordingly.
    old_notetype: Notetype = M.notetype(note.note_type())
    return update_note(note, decknote, old_notetype, new_notetype)


@beartype
def get_header_lines(colnote) -> List[str]:
    """Get header of markdown representation of note."""
    lines = [
        "# Note",
        "```",
        f"guid: {colnote.n.guid}",
        f"notetype: {colnote.notetype.name}",
        "```",
        "",
        "### Tags",
        "```",
    ]
    lines += colnote.n.tags
    lines += ["```", ""]
    return lines


@curried
@beartype
def localmedia(s: str, regex: str) -> Iterable[str]:
    """Return local media filenames matching the given regex pattern."""
    fnames = map(lambda m: m.group("fname"), re.finditer(regex, s))
    fnames = map(lambda s: s.strip(), fnames)
    return filter(lambda x: not re.match(URLS, x.lower()), fnames)


@beartype
def media_filenames_in_field(col: Collection, s: str) -> Iterable[str]:
    """A copy of `MediaManager.files_in_str()`, but without LaTeX rendering."""
    s = (s.strip()).replace('"', "")
    return F.cat(map(localmedia(s), col.media.regexps))


@curried
@beartype
def copy_note_media(
    col: Collection, src: Dir, tgt: Dir, row: NoteDBRow
) -> FrozenSet[File]:
    """
    Copy a single note's media files and return the copies as a set. We do this
    by first filtering for only 'rootfiles', i.e. excluding media files in
    subdirectories of the media directory. Then we take only those which exist,
    i.e. typecheck as `File`. Then we construct the source and destination
    paths, and finally actually perform the copy op, returning the result.

    Note that `src` is the media directory where the files originate, and `tgt`
    is the media directory we're copying to.
    """
    files: Iterable[str] = media_filenames_in_field(col, row.flds)
    rootfiles = filter(lambda f: f == os.path.basename(f), files)
    medias: Iterable[File] = filter(F.isfile, map(lambda f: F.chk(src / f), rootfiles))
    srcdsts = map(lambda file: (file, F.chk(tgt / file.name)), medias)
    return frozenset(starmap(F.copyfile, srcdsts))


@curried
@beartype
def copy_notetype_media(
    src: Dir, tgt: Dir, paths: Set[Path], m: NotetypeDict
) -> FrozenSet[File]:
    """Copy media from notetype `m` from source to target, returning set of copies."""
    matches: Iterable[Path] = filter(lambda p: hasmedia(m, str(p)), paths)
    medias = filter(F.isfile, map(lambda p: F.chk(src / p), matches))
    srcdsts = map(lambda f: (f, F.chk(tgt / f.name)), medias)
    return frozenset(starmap(F.copyfile, srcdsts))


@beartype
def copy_media_files(
    col: Collection,
    media_target_dir: EmptyDir,
) -> Dict[int, Set[File]]:
    """
    Get a list of extant media files used in notes and notetypes, copy those
    media files to the top-level `_media/` directory in the repository root,
    and return a map sending note ids to sets of copied media files.

    Adapted from code in `anki/pylib/anki/exporting.py`. Specifically, the
    `AnkiExporter.exportInto()` function.

    Parameters
    ----------
    col
        Anki collection.
    media_target_dir
        Where media files are to be copied to.
    """
    # All note ids as a string for the SQL query.
    strnids = ids2str(list(col.find_notes(query="")))

    # This is the path to the media directory. In the original implementation
    # of `AnkiExporter.exportInto()`, there is check made of the form
    #
    #   if self.mediaDir:
    #
    # before doing path manipulation with this string.
    #
    # Examining the `__init__()` function of `MediaManager`, we can see that
    # `col.media.dir()` will only be `None` in the case where `server=True` is
    # passed to the `Collection` constructor. But since we do the construction
    # within ki, we have a guarantee that this will never be true, and thus we
    # can assume it is a nonempty string, which is all we need for the
    # following code to be safe.
    media_dir = F.chk(Path(col.media.dir()))
    if not isinstance(media_dir, Dir):
        raise MissingMediaDirectoryError(col.path, media_dir)

    # Find media files that appear in note fields and copy them to the target.
    query: str = "select * from notes where id in " + strnids
    rows: List[NoteDBRow] = [NoteDBRow(*row) for row in col.db.all(query)]
    rows = TQ(rows, "Media")
    copy_fn = copy_note_media(col, media_dir, media_target_dir)
    media = {row.nid: copy_fn(row) for row in rows}
    mids = col.db.list("select distinct mid from notes where id in " + strnids)

    # Copy notetype template media files.
    _, _, files = F.shallow_walk(media_dir)
    paths: Iterable[Path] = map(lambda f: Path(f.name), files)
    paths = set(filter(lambda f: str(f).startswith("_"), paths))
    models = filter(lambda m: int(m["id"]) in mids, col.models.all())

    mediasets = map(copy_notetype_media(media_dir, media_target_dir, paths), models)
    media[NOTETYPE_NID] = reduce(lambda x, y: x.union(y), mediasets, set())

    return media


@beartype
def hasmedia(model: NotetypeDict, fname: str) -> bool:
    """
    Check if a notetype has media.

    Adapted from `anki.exporting.AnkiExporter._modelHasMedia()`, which is an
    instance method, but does not make any use of `self`, and so could be a
    staticmethod. It is a pure function.
    """
    # First check the styling.
    if fname in model["css"]:
        return True
    # If no reference to fname then check the templates as well.
    return any(map(lambda t: fname in t["qfmt"] or fname in t["afmt"], model["tmpls"]))


@beartype
def write_repository(
    col: Collection,
    targetdir: Dir,
    dotki: DotKi,
    media_target_dir: EmptyDir,
) -> Collection:
    """Write notes to appropriate directories in `targetdir`."""
    # Create config file.
    config = configparser.ConfigParser()
    config["remote"] = {"path": col.path}
    with open(dotki.config, "w", encoding=UTF8) as config_f:
        config.write(config_f)

    # ColNote-containing data structure, to be passed to `write_decks()`.
    nids: Iterable[int] = TQ(col.find_notes(query=""), "Notes")
    colnotes: Dict[int, ColNote] = {nid: M.colnote(col, nid) for nid in nids}
    media: Dict[int, Set[File]] = copy_media_files(col, media_target_dir)

    write_decks(
        col=col,
        targetdir=targetdir,
        colnotes=colnotes,
        media=media,
    )
    return col


@beartype
def postorder(node: Union[Root, Deck]) -> List[Deck]:
    """
    Post-order traversal. Guarantees that we won't process a node until we've
    processed all its children.
    """
    descendants: List[Deck] = reduce(lambda xs, x: xs + postorder(x), node.children, [])
    return descendants if isinstance(node, Root) else descendants + [node]


@beartype
def preorder(node: Union[Root, Deck]) -> List[Deck]:
    """
    Pre-order traversal. Guarantees that we won't process a node until
    we've processed all its ancestors.
    """
    descendants: List[Deck] = reduce(lambda xs, x: xs + preorder(x), node.children, [])
    return descendants if isinstance(node, Root) else [node] + descendants


@beartype
def write_decks(
    col: Collection,
    targetdir: Dir,
    colnotes: Dict[int, ColNote],
    media: Dict[int, Set[File]],
) -> None:
    """
    The proper way to do this is a DFS traversal, perhaps recursively, which
    will make it easier to keep things purely functional, accumulating the
    model ids of the children in each node. For this, we must construct a tree
    from the deck names.

    Implement new `ColNote`-writing procedure, using `DeckTreeNode`s.

    It must do the following for each deck:
    - create the deck directory
    - write the models.json file
    - create and populate the media directory
    - write the note payload for each note in the correct deck, exactly once

    In other words, for each deck, we need to write all of its:
    - models
    - media
    - notes

    The first two are cumulative: we want the models and media of subdecks to
    be included in their ancestors. The notes, however, should not be
    cumulative. Indeed, we want each note to appear exactly once in the
    entire repository, making allowances for the case where a single note's
    cards are spread across multiple decks, in which case we must create a
    symlink.

    And actually, both of these cases are nicely taken care of for us by the
    `DeckManager.cids()` function, which has a `children: bool` parameter
    which toggles whether or not to get the card ids of subdecks or not.
    """
    # Accumulate pairs of model ids and notetype maps. The return type of the
    # `ModelManager.get()` call below indicates that it may return `None`,
    # but we know it will not because we are getting the notetype id straight
    # from the Anki DB.
    #
    # Dump the models file for the whole repository.
    models = {m.id: col.models.get(m.id) for m in col.models.all_names_and_ids()}
    with open(targetdir / MODELS_FILE, "w", encoding=UTF8) as f:
        json.dump(models, f, ensure_ascii=False, indent=4, sort_keys=True)

    # Construct an iterable of all decks except the trivial deck.
    root: Deck = M.tree(col, targetdir, col.decks.deck_tree())
    collisions, decks = F.part(lambda d: MEDIA in d.fullname, postorder(root))
    if any(True for _ in collisions):
        warn(MediaDirectoryDeckNameCollisionWarning())
    decks = list(decks)
    deckmap = {d.fullname: d for d in decks}

    # Write cards, models, and media to filesystem.
    do(write_note(deckmap), TQ(colnotes.values(), "Notes"))
    do(write_models(col, models), TQ(decks, "Notetypes"))
    symlink_media(col, root, targetdir, media)


@curried
@beartype
def write_note(
    deckmap: Dict[str, Deck],
    colnote: ColNote,
) -> File:
    decknames = set(map(lambda c: c.col.decks.name(c.did), colnote.n.cards()))
    sortf = colnote.sfld
    if len(decknames) == 0:
        raise ValueError(f"No cards for note: {sortf}")
    if len(decknames) > 1:
        raise ValueError(f"Cards for note {sortf} are in distinct decks: {decknames}")
    fullname = decknames.pop()
    parts = fullname.split("::")
    if "_media" in parts:
        raise ValueError(f"Bad deck name '{fullname}' (cannot contain '_media')")
    deck: Deck = deckmap[fullname]
    path: NoFile = get_note_path(colnote, deck.deckd)
    payload: str = get_note_payload(colnote)
    return F.write(path, payload)


@curried
@beartype
def write_models(col: Collection, models: Dict[int, NotetypeDict], deck: Deck) -> None:
    """Write the `models.json` file for the given deck."""
    did: int = deck.did
    deckd: Dir = deck.deckd
    descendants: List[CardId] = col.decks.cids(did=did, children=True)
    cards: List[Card] = list(map(col.get_card, descendants))
    descendant_mids: Set[int] = {c.note().mid for c in cards}

    # Write `models.json` for current deck.
    deck_models = {mid: models[mid] for mid in descendant_mids}
    with open(deckd / MODELS_FILE, "w", encoding=UTF8) as f:
        json.dump(deck_models, f, ensure_ascii=False, indent=4, sort_keys=True)


@beartype
def mklink(targetd: Dir, colnote: ColNote, deckd: Dir, card: Card, file: File) -> None:
    """Return a windows link for a card if one is necessary."""
    note_path: NoFile = get_note_path(colnote, deckd, card.template()["name"])
    M.link(targetd, PlannedLink(link=note_path, tgt=file))


@beartype
def parentmap(root: Union[Root, Deck]) -> Dict[str, Union[Root, Deck]]:
    """Map deck fullnames to parent `Deck`s."""
    parents = {child.fullname: root for child in root.children}
    return parents | reduce(lambda x, y: x | y, map(parentmap, root.children), {})


@curried
@beartype
def planned_link(
    parents: Dict[str, Union[Root, Deck]], deck: Deck, media_file: File
) -> Optional[PlannedLink]:
    """Get the target of the to-be-created media symlink."""
    link: Path = F.chk(deck.mediad / media_file.name, resolve=False)
    if not isinstance(link, NoFile):
        return None

    parent: Union[Root, Deck] = parents[deck.fullname]
    if isinstance(parent, Root):
        tgt = media_file
    else:
        tgt = F.chk(parent.mediad / media_file.name, resolve=False)
    return PlannedLink(link=link, tgt=tgt)


@curried
@beartype
def symlink_deck_media(
    col: Collection,
    targetd: Dir,
    media: Dict[int, Set[File]],
    parents: Dict[str, Union[Root, Deck]],
    deck: Deck,
) -> None:
    """Create chained symlinks for a single deck."""
    # Get nids for all descendant notes with media.
    descendants: List[CardId] = col.decks.cids(did=deck.did, children=True)
    cards: Iterable[Card] = map(col.get_card, descendants)
    nids: Set[NoteId] = {NOTETYPE_NID} | set(map(lambda c: c.nid, cards))

    # Get link path and target for each media file, and create the links.
    files = F.cat(map(lambda nid: media[nid], filter(lambda nid: nid in media, nids)))
    plinks = filter(None, map(planned_link(parents, deck), files))
    do(M.link(targetd), plinks)


@beartype
def symlink_media(
    col: Collection,
    root: Root,
    targetd: Dir,
    media: Dict[int, Set[File]],
) -> None:
    """Chain symlinks up the deck tree into top-level `<collection>/_media/`."""
    decks: List[Deck] = preorder(root)
    parents: Dict[str, Union[Root, Deck]] = parentmap(root)
    return do(symlink_deck_media(col, targetd, media, parents), decks)


@beartype
def html_to_screen(html: str) -> str:
    """
    Convert html for a *single field* into plaintext, to be displayed within a
    markdown file.

    Does very litle (just converts HTML-escaped special characters like `<br>`
    tags or `&nbsp;`s to their UTF-8 equivalents).
    """
    html = re.sub(r"\<style\>.*\<\/style\>", "", html, flags=re.S)
    plain = html

    # For convenience: Un-escape some common LaTeX constructs.
    plain = plain.replace(r"\\\\", r"\\")
    plain = plain.replace(r"\\{", r"\{")
    plain = plain.replace(r"\\}", r"\}")
    plain = plain.replace(r"\*}", r"*}")

    plain = plain.replace(r"&lt;", "<")
    plain = plain.replace(r"&gt;", ">")
    plain = plain.replace(r"&amp;", "&")
    plain = plain.replace(r"&nbsp;", " ")

    plain = plain.replace("<br>", "\n")
    plain = plain.replace("<br/>", "\n")
    plain = plain.replace("<br />", "\n")

    # Unbreak lines within src attributes.
    plain = re.sub('src= ?\n"', 'src="', plain)

    plain = re.sub(r"\<b\>\s*\<\/b\>", "", plain)
    return plain


@curried
@beartype
def get_field_payload(col: Collection, name: str, content: str) -> List[str]:
    """Get the lines of a markdown snippet for some Anki note field."""
    text = col.media.escape_media_filenames(html_to_screen(content), unescape=True)
    return [f"## {name}", text, ""]


@beartype
def get_note_payload(colnote: ColNote) -> str:
    """
    Return the markdown-converted contents of the Anki note represented by
    `colnote` as a string.

    A `ColNote` is a dataclass wrapper around a `Note` object which has been
    loaded from the DB.
    """
    lines = get_header_lines(colnote)
    lines += F.cat(starmap(get_field_payload(colnote.n.col), colnote.n.items()))
    return "\n".join(lines)


@beartype
def git_pull(remote: str, branch: str, cwd: Dir) -> str:
    """Pull remote into branch using a subprocess call."""
    args = ["git", "pull", "-v", remote, branch]
    p = subprocess.run(args, check=False, cwd=cwd, capture_output=True)
    return f"{p.stdout.decode()}\n{p.stderr.decode()}"


@beartype
def echo(string: str, silent: bool = False) -> None:
    """Call `click.secho()` with formatting."""
    if not silent:
        click.secho(string, bold=True)


@beartype
def warn(w: Warning) -> None:
    """Call `click.secho()` with formatting (yellow)."""
    click.secho(f"WARNING: {str(w)}", bold=True, fg="yellow")


@beartype
def get_target(cwd: Dir, col_file: File, directory: str) -> Tuple[EmptyDir, bool]:
    """Create default target directory."""
    path = F.chk(Path(directory) if directory != "" else cwd / col_file.stem)
    new: bool = True
    if isinstance(path, NoPath):
        # If there is no directory at "path", then create it.
        path.mkdir(parents=True)
        return M.emptydir(path), new
    if isinstance(path, EmptyDir):
        new = False
        return path, new
    # Otherwise, the path already exists, so raise an error.
    raise TargetExistsError(path)


@beartype
def echo_note_change_types(deltas: Iterable[Delta]) -> None:
    """Write a table of git change types for notes to stdout."""
    # pylint: disable=too-many-locals
    is_change_type = lambda t: lambda d: d.status == t

    vs, ws, xs, ys, zs = tee(deltas, 5)
    adds = list(filter(is_change_type(ADDED), vs))
    deletes = list(filter(is_change_type(DELETED), ws))
    renames = list(filter(is_change_type(RENAMED), xs))
    modifies = list(filter(is_change_type(MODIFIED), ys))
    types = list(filter(is_change_type(TYPECHANGED), zs))

    # Define left- and right-padding
    LPAD, RPAD = 15, 9
    add_info: str = "ADD".ljust(LPAD) + str(len(adds)).rjust(RPAD)
    delete_info: str = "DELETE".ljust(LPAD) + str(len(deletes)).rjust(RPAD)
    modification_info: str = "MODIFY".ljust(LPAD) + str(len(modifies)).rjust(RPAD)
    rename_info: str = "RENAME".ljust(LPAD) + str(len(renames)).rjust(RPAD)
    type_info: str = "TYPE CHANGE".ljust(LPAD) + str(len(types)).rjust(RPAD)

    echo("=" * (LPAD + RPAD))
    echo("Note change types")
    echo("-" * (LPAD + RPAD))
    echo(f"{add_info}\n{delete_info}\n{modification_info}\n{rename_info}\n{type_info}")
    echo("=" * (LPAD + RPAD))


@curried
@beartype
def add_model(col: Collection, model: Notetype) -> None:
    """Add a model to the database."""
    # Check if a model already exists with this name, and get its `mid`.
    mid: Optional[int] = col.models.id_for_name(model.name)

    # TODO: This function is unfinished. We need to add new notetypes (and
    # rename them) only if they are 'new', where new means they are different
    # from anything else already in the DB, in the content-addressed sense. If
    # they are new, then we must indicate that the notes we are adding actually
    # have these new notetypes. For this, it may make sense to use the hash of
    # the notetype everywhere (i.e. in the note file) rather than the name or
    # mid.
    #
    # If a model already exists with this name, parse it, and check if its hash
    # is identical to the model we are trying to add.
    if mid is not None:
        nt: NotetypeDict = col.models.get(mid)

        # If we are trying to add a model that has the exact same content and
        # name as an existing model, skip it.
        existing: Notetype = M.notetype(nt)
        if notetype_json(model) == notetype_json(existing):
            return

        # If the hashes don't match, then we somehow need to update
        # `decknote.model` for the relevant notes.
        warn(NotetypeCollisionWarning(model, existing))

    nt_copy: NotetypeDict = copy.deepcopy(model.dict)
    nt_copy["id"] = 0
    changes: OpChangesWithId = col.models.add_dict(nt_copy)
    nt: NotetypeDict = col.models.get(changes.id)
    model: Notetype = M.notetype(nt)
    echo(f"Added model '{model.name}'")


@beartype
def mediadata(col: Collection, fname: str) -> bytes:
    """Get media file content as bytes (empty if missing)."""
    if not col.media.have(fname):
        return b""
    path = os.path.join(col.media.dir(), fname)
    try:
        with open(path, "rb") as f:
            return f.read()
    except OSError:
        return b""


@beartype
def get_note_metadata(col: Collection) -> Dict[str, NoteMetadata]:
    """
    Construct a map from guid -> (nid, mod, mid), adapted from
    `Anki2Importer._import_notes()`. Note that `mod` is the modification
    timestamp, in epoch seconds (timestamp of when the note was last modified).
    """
    guids: Dict[str, NoteMetadata] = {}
    for nid, guid, mod, mid in col.db.execute("select id, guid, mod, mid from notes"):
        guids[guid] = NoteMetadata(nid, mod, mid)
    return guids


@curried
@beartype
def mediabytes(col: Collection, file: File) -> MediaBytes:
    """Get old bytes (from collection) and new bytes (from file) for media file."""
    old: bytes = mediadata(col, file.name)
    new: bytes = file.read_bytes()
    return MediaBytes(file=file, old=old, new=new)


@curried
@beartype
def addmedia(col: Collection, m: MediaBytes) -> AddedMedia:
    """Add a media file to collection (possibly renaming)."""
    return AddedMedia(file=m.file, new_name=col.media.add_file(m.file))


@beartype
def commit_hashes_file(kirepo: KiRepo) -> None:
    """Add and commit hashes file."""
    kirepo.repo.index.add(f"{KI}/{HASHES_FILE}")
    kirepo.repo.index.commit("Update collection hashes file.")


@beartype
def cleanup(targetdir: Dir, new: bool) -> Union[Dir, EmptyDir, NoPath]:
    """Cleans up after failed clone operations."""
    try:
        if new:
            return F.rmtree(targetdir)
        _, dirs, files = F.shallow_walk(targetdir)
        do(F.rmtree, dirs)
        do(os.remove, files)
    except PermissionError as _:
        pass
    return F.chk(targetdir)


@click.group()
@click.version_option()
@beartype
def ki() -> None:
    """
    The universal CLI entry point for `ki`.

    Takes no arguments, only has three subcommands (clone, pull, push).
    """
    return


@ki.command()
@click.argument("collection", required=False, default="")
@click.argument("directory", required=False, default="")
@click.argument("profile", required=False, default="")
def clone(collection: str, directory: str = "", profile: str = None) -> None:
    """Clone an Anki collection into a directory."""
    if profile:
        OS = platform.system()
        if OS == "Windows":
            profile_path = Path(os.getenv('APPDATA'), "Anki2", profile)
            # profile_path = F.chk(Path(os.getenv('APPDATA'), "Anki2", profile))
        elif OS == "Linux":
            profile_path = Path("~", ".local", "share", "Anki2")
        elif OS == "Darwin":
            profile_path = Path("~", "Library", "Application Support", "Anki2")
        else:
            raise ValueError('This OS is not recognized. The path to the Anki collection cannot be automatically detected. Please provide the directory explicitly.')
        
        collection_path = Path(profile_path, "collection.anki2")

        # Check that the profile directory and collection file exist.
        if not profile_path.is_dir():
            raise IOError(f"The profile \"{profile}\" does not exist at {profile_path}.")
        if not collection_path.is_file():
            raise IOError(f"The file  \"collection.anki2\" does not exist at {collection_path}.")
        
        collection = str(collection_path)
    _clone1(collection, directory)


@beartype
def _clone1(collection: str, directory: str = "") -> git.Repo:
    """Execute a clone operations."""
    col_file: File = M.xfile(Path(collection))
    # Write all files to `targetdir`, and instantiate a `KiRepo` object.
    targetdir, new = get_target(F.cwd(), col_file, directory)
    try:
        col = M.collection(col_file)
        _, _ = _clone2(col, targetdir, msg="Initial commit", silent=False)
        col.close(save=False)
        kirepo: KiRepo = M.kirepo(targetdir)
        kirepo.repo.create_tag(LCA)
        kirepo.repo.close()
        gc.collect()
        return kirepo.repo
    except Exception as err:
        cleanup(targetdir, new)
        raise err


@beartype
def _clone2(
    col: Collection,
    targetdir: EmptyDir,
    msg: str,
    silent: bool,
) -> Tuple[git.Repo, str]:
    """
    Clone an Anki collection into a directory.

    The caller expects that `targetdir` will be the root of a valid ki
    repository after this function is called, so we need to do our repo
    initialization with gitpython in here, as opposed to in `clone()`.

    Parameters
    ----------
    col : Collection
        An anki collection object.
    targetdir : pathlib.Path
        A path to a directory to clone the collection into.
        Note: we check that this directory is empty.
    msg : str
        Message for initial commit.
    silent : bool
        Whether to suppress progress information printed to stdout.

    Returns
    -------
    repo : git.Repo
        The cloned repository.
    branch_name : str
        The name of the default branch.
    """
    col_file: File = M.xfile(Path(col.path))
    kidir, mediadir = M.empty_kirepo(targetdir)
    dotki: DotKi = M.dotki(kidir)
    md5sum = F.md5(col_file)
    echo(f"Cloning into '{targetdir}'...", silent=silent)
    (targetdir / GITIGNORE_FILE).write_text(f"{KI}/{BACKUPS_DIR}\n")
    (targetdir / GITATTRS_FILE).write_text("*.md linguist-detectable\n")

    # Write note files to disk.
    write_repository(col, targetdir, dotki, mediadir)
    repo, branch = F.init(targetdir)

    # Store a checksum of the Anki collection file in the hashes file.
    append_md5sum(kidir, col_file.name, md5sum)

    F.commitall(repo, msg)
    if repo.is_dirty():
        raise NonEmptyWorkingTreeError(repo)
    return repo, branch


@ki.command()
@beartype
def pull() -> None:
    """Pull changes into the current ki repository from an Anki collection."""
    _pull1()


@beartype
def _pull1() -> None:
    """Execute a pull op."""
    # Check that we are inside a ki repository, and get the associated collection.
    kirepo: KiRepo = M.kirepo(F.cwd())
    col = M.collection(kirepo.col_file)
    md5sum: str = F.md5(kirepo.col_file)
    hashes: List[str] = kirepo.hashes_file.read_text(encoding=UTF8).split("\n")
    hashes = list(filter(lambda l: l != "", hashes))
    if md5sum in hashes[-1]:
        echo("ki pull: up to date.")
        col.close(save=False)
        return

    col = _pull2(kirepo, col)
    col.close(save=False)


@beartype
def _pull2(kirepo: KiRepo, col: Collection) -> Collection:
    """
    Pull into `kirepo` without checking if we are already up-to-date.

    Load the git repository at `anki_remote_root`, force pull (preferring
    'theirs', i.e. the new stuff from the sqlite3 database) changes from that
    repository (which is cloned straight from the collection, which in general
    may have new changes) into `lca_repo`, and then pull `lca_repo` into the
    main repository.

    We pull in this sequence in order to avoid merge conflicts. Since we first
    pull into a snapshot of the repository as it looked when we last pushed to
    the database, we know that there cannot be any merge conflicts, because to
    git, it just looks like we haven't made any changes since then. Then we
    pull the result of that merge into our actual repository. So there could
    still be merge conflicts at that point, but they will only be 'genuine'
    merge conflicts in some sense, because as a result of using this snapshot
    strategy, we give the anki collection the appearance of being a persistent
    remote git repo. If we didn't do this, the fact that we did a fresh clone
    of the database every time would mean that everything would look like a
    merge conflict, because there is no shared history.

    Parameters
    ----------
    kirepo : KiRepo
        A dataclass representing the Ki repository in the cwd.

    Raises
    ------
    CollectionChecksumError
        If the Anki collection file was modified while pulling changes. This is
        very unlikely, since the caller acquires a lock on the SQLite3
        database.
    """
    # pylint: disable=too-many-locals
    md5sum: str = F.md5(kirepo.col_file)

    # Copy `repo` into a temp directory and `reset --hard` at rev of last
    # successful `push()`, which is the last common ancestor, or 'LCA'.
    head: Rev = M.head(kirepo.repo)
    rev: Rev = M.rev(kirepo.repo, sha=kirepo.repo.tag(LCA).commit.hexsha)
    lca_repo: git.Repo = cp_repo(rev, f"{LOCAL_SUFFIX}-{md5sum}")

    # Clone collection into a temp directory at `anki_remote_root`.
    anki_remote_root: EmptyDir = F.mksubdir(F.mkdtemp(), REMOTE_SUFFIX / md5sum)
    msg = f"Fetch changes from DB at `{kirepo.col_file}` with md5sum `{md5sum}`"
    remote_repo, branch = _clone2(col, anki_remote_root, msg, silent=False)

    # Create git remote pointing to `remote_repo`, which represents the current
    # state of the Anki SQLite3 database, and pull it into `lca_repo`.
    anki_remote = lca_repo.create_remote(REMOTE_NAME, F.gitd(remote_repo))
    anki_remote.fetch()

    # Handle deleted files, preferring `theirs`.
    diffidx = lca_repo.commit("HEAD").diff(lca_repo.commit("FETCH_HEAD"))
    dels: Iterable[git.Diff] = diffidx.iter_change_type(DELETED.value)
    dels = filter(lambda d: d.a_path != GITMODULES_FILE, dels)
    dels = filter(lambda d: F.isfile(F.chk(F.root(lca_repo) / d.a_path)), dels)
    a_paths: Iterable[str] = set(map(F.git_rm(lca_repo), map(lambda d: d.a_path, dels)))

    if len(a_paths) > 0:
        details: str = "".join(map(lambda a: f"Remove '{a}'\n", a_paths))
        F.commitall(lca_repo, msg=f"Remove files deleted in remote.\n\n{details}")

    remote_root: Dir = F.root(remote_repo)
    lca_repo = M.gitcopy(lca_repo, remote_root, unsub=False)
    F.commitall(lca_repo, f"Pull changes from repository at `{remote_root}`")

    # Create remote pointing to `lca_repo` and pull into `repo`. Note
    # that this `git pull` may not always create a merge commit, because a
    # fast-forward only updates the branch pointer.
    lca_remote = kirepo.repo.create_remote(REMOTE_NAME, lca_repo.git_dir)
    kirepo.repo.git.config("pull.rebase", "false")
    out = git_pull(REMOTE_NAME, branch, kirepo.root)
    echo(out)
    kirepo.repo.delete_remote(lca_remote)

    # The merge will have overwritten the hashes file with only the collection
    # hash from the fresh clone of the remote, so we checkout its state from
    # before the merge.
    kirepo.repo.git.checkout([head.sha, "--", f"{KI}/{HASHES_FILE}"])

    # Raise an error if the collection was modified during pull.
    if F.md5(kirepo.col_file) != md5sum:
        raise CollectionChecksumError(kirepo.col_file)

    # Append the hash of the collection to the hashes file.
    if "Aborting" not in out:
        append_md5sum(kirepo.ki, kirepo.col_file.name, md5sum)
        commit_hashes_file(kirepo)

    return col


# PUSH


@ki.command()
@beartype
def push() -> None:
    """Push commits from the currrent ki repository into an Anki collection."""
    _push()


@beartype
def _push() -> PushResult:
    """Execute a push op."""
    # pylint: disable=too-many-locals
    # Check that we are inside a ki repository, and load collection.
    kirepo: KiRepo = M.kirepo(F.cwd())
    col = M.collection(kirepo.col_file)
    md5sum: str = F.md5(kirepo.col_file)
    hashes: List[str] = kirepo.hashes_file.read_text(encoding=UTF8).split("\n")
    hashes = list(filter(lambda l: l != "", hashes))
    if md5sum not in hashes[-1]:
        raise UpdatesRejectedError(kirepo.col_file)

    head_kirepo: KiRepo = cp_ki(M.head_ki(kirepo), f"{HEAD_SUFFIX}-{md5sum}")
    remote_root: EmptyDir = F.mksubdir(F.mkdtemp(), REMOTE_SUFFIX / md5sum)

    msg = f"Fetch changes from collection '{kirepo.col_file}' with md5sum '{md5sum}'"
    remote_repo, _ = _clone2(col, remote_root, msg, silent=True)

    remote_repo = M.gitcopy(remote_repo, head_kirepo.root, unsub=True)
    F.commitall(remote_repo, f"Pull changes from repository at `{kirepo.root}`")

    parse: Callable[[Delta], DeckNote] = parse_note(*M.parser_and_transformer())
    deltas, warnings = F.part(lambda x: isinstance(x, Delta), diff2(remote_repo, parse))
    do(warn, warnings)

    # If there are no changes, quit.
    if len(set(deltas)) == 0:
        echo("ki push: up to date.")
        col.close(save=False)
        return PushResult.UP_TO_DATE

    echo(f"Pushing to '{kirepo.col_file}'")
    models: Dict[str, Notetype] = get_models_recursively(head_kirepo)
    return write_collection(deltas, models, kirepo, parse, head_kirepo, col)


@beartype
def write_collection(
    deltas: Iterable[Delta],
    models: Dict[str, Notetype],
    kirepo: KiRepo,
    parse: Callable[[Delta], DeckNote],
    head_kirepo: KiRepo,
    col: Collection,
) -> PushResult:
    """Push a list of `Delta`s to an Anki collection."""
    # pylint: disable=too-many-locals
    # Copy collection to a temp directory.
    temp_col_dir: Dir = F.mkdtemp()
    new_col_file = temp_col_dir / kirepo.col_file.name
    col_name: str = kirepo.col_file.name
    new_col_file: NoFile = F.chk(temp_col_dir / col_name)
    new_col_file: File = F.copyfile(kirepo.col_file, new_col_file)

    # Open collection and add new models to root `models.json` file.
    tempcol: Collection = M.collection(new_col_file)
    do(add_model(tempcol), models.values())

    # Stash both unstaged and staged files (including untracked).
    head_kirepo.repo.git.stash(include_untracked=True, keep_index=True)
    head_kirepo.repo.git.reset("HEAD", hard=True)

    # Display table of note change type counts and partition deltas into
    # 'deletes' and 'not deletes'.
    xs, ys, zs = tee(deltas, 3)
    echo_note_change_types(xs)
    dels: Iterable[Delta] = filter(lambda d: d.status == DELETED, ys)
    deltas: Iterable[Delta] = filter(lambda d: d.status != DELETED, zs)

    # Map guid -> (nid, mod, mid).
    guids: Dict[str, NoteMetadata] = get_note_metadata(tempcol)

    # Parse to-be-deleted notes and remove them from collection.
    del_guids: Iterable[str] = map(lambda dd: dd.guid, map(parse, dels))
    del_guids = set(filter(lambda g: g in guids, del_guids))
    del_nids: Iterable[NoteId] = map(lambda g: guids[g].nid, del_guids)
    tempcol.remove_notes(list(del_nids))

    # Push changes for all other notes.
    guids = {k: v for k, v in guids.items() if k not in del_guids}
    timestamp_ns: int = time.time_ns()
    new_nids: Iterator[int] = itertools.count(int(timestamp_ns / 1e6))
    decknotes: Iterable[DeckNote] = map(parse, deltas)
    do(warn, F.cat(map(push_note(tempcol, timestamp_ns, guids, new_nids), decknotes)))

    # It is always safe to save changes to the DB, since the DB is a copy.
    tempcol.close(save=True)

    # Backup collection file and overwrite collection.
    backup(kirepo)
    F.copyfile(new_col_file, kirepo.col_file)
    echo(f"Overwrote '{kirepo.col_file}'")

    # Add media files to collection.
    media_files = F.rglob(head_kirepo.root, MEDIA_FILE_RECURSIVE_PATTERN)
    mbytes: Iterable[MediaBytes] = map(mediabytes(col), media_files)

    # Skip media files whose twin in collection has same name and same data.
    mbytes = filter(lambda m: m.old == b"" or m.old != m.new, mbytes)

    # Add (and possibly rename) media paths.
    renames = filter(lambda a: a.file.name != a.new_name, map(addmedia(col), mbytes))
    warnings = map(lambda r: RenamedMediaFileWarning(r.file.name, r.new_name), renames)
    do(warn, warnings)
    col.close(save=True)

    # Append and commit collection checksum to hashes file.
    append_md5sum(kirepo.ki, kirepo.col_file.name, F.md5(kirepo.col_file))
    commit_hashes_file(kirepo)

    # Update commit SHA of most recent successful PUSH and unlock SQLite DB.
    kirepo.repo.delete_tag(LCA)
    kirepo.repo.create_tag(LCA)
    return PushResult.NONTRIVIAL
