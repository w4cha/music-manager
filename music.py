from pathlib import Path
from mutagen import MutagenError, File, FileType
from data import Database, DatabaseError
from datetime import datetime
from typing import Generator
# future write to db directly, check if song is in database and finally write to path from
# db to fill up the required information
# mark deleted song as removed_entry_free_space so new song can use that space first before
# creating new entries
# C:\Users\Lucas Folch\Desktop\txt\path_final.txt
# C:\Users\Lucas Folch\Desktop\temporary
import sys
import time
import re
import csv
import platform
import pandas as pd

"""
table creation query:

CREATE TABLE [music data] (
    count          INTEGER PRIMARY KEY
                           UNIQUE,
    title          TEXT    NOT NULL
                           DEFAULT unknown,
    artist         TEXT    NOT NULL
                           DEFAULT unknown,
    album          TEXT    DEFAULT unknown
                           NOT NULL,
    albumartist    TEXT    DEFAULT unknown
                           NOT NULL,
    genre          TEXT    DEFAULT unknown
                           NOT NULL,
    length_seconds NUMERIC NOT NULL
                           DEFAULT (0.0),
    path           TEXT    NOT NULL
                           DEFAULT unknown_path
                           COLLATE NOCASE
                           UNIQUE
);
"""

# read paths form txt file and parsed them
# to remove the trailing "" or '' when copied from explorer
def check_copy_path(path_str: str) -> str:
    if platform.system() == "Windows" and re.match(r'^"[cCeE]:(?:(?:\\|\\\\)[^?/*<>|:\\"]+)+"$',
                                                   path_str) is not None:
        return path_str[1:-1]
    return path_str


def get_files(directory: Path, song_count: int) -> int:
    file_list = list(directory.iterdir())
    for sub_path in file_list:
        if sub_path.is_dir():
            song_count = get_files(sub_path, song_count)
        elif sub_path.is_file() and sub_path.parts[-1][0:2] != "._":
            write_metadata(sub_path, song_count)
            song_count += 1
    return song_count

# returns the last not backed entry
# if 0 means no data was backed up
# if -1 all data was logged
# if -2 the data was logged but in a different file
# else it returns the last item to be logged
# should not be able to use this before
# any entries are marked as deleted
# position is used to control how
# the log is written if you have
# multiple entries you want to log
# but do no want them to appears separately
def write_log(entry_number: int, file_limit_size: int = 100_000_000, position: tuple[int, str] = (1, "none")) -> int:
    if not get_total():
        return 0
    needs_backup = False
    current_entry = ""
    with Database(working_dict) as to_log:
        with open(txt_log, "a", encoding="utf-8") as log:
            try:
                next_log = next(to_log.read_db("SELECT * FROM 'music data' WHERE count = {0}".format(entry_number), as_row=True))
            except StopIteration:
                return entry_number  
            else:
                log_dict = dict(next_log)
            current_entry += f"Change {position[0]}: {' '.join([f'{key}: {value}' for key, value in log_dict.items()])}\n"
            if Path(txt_log).stat().st_size <= file_limit_size:
                    if position[-1] in ("start", "none"):
                        log.write("-" * 50)
                        log.write(f"\nDay and time of changing: {datetime.now()}\n")
                    log.write(current_entry)
            else:
                # create file name like log_old_before_str(datetime.now().date()) and copy the context 
                # of current file to the new log
                print("Log memory is going to be transcribe to a new file due to\n txt file surpassing the 100mb size")
                log.write("-" * 50)
                log.write("\n")
                needs_backup = True
            if position[-1] in ("end", "none") and not needs_backup:
                log.write("-" * 50)
                log.write("\n")
    if needs_backup:
        # backing_up data:
        get_current_log_files = sum([1 for _ in Path(fr"{Path(__file__).parent}\log").iterdir()]) - 1
        new_file: str = fr"{Path(__file__).parent}\log\log_old_before_{get_current_log_files}_{datetime.now().date()}.txt"
        # creating the file
        with open(new_file, "w", encoding="utf-8") as _:
            pass
        print("previous log entry locations:\n{new_file}")
        with open(txt_log, "r", encoding="utf-8") as old:
            with open(new_file, "a", encoding="utf-8") as new:
                for line in old:
                    new.write(line)
        # deleting old data
        # and writing leftover data
        with open(txt_log, "w", encoding="utf-8") as first_entry:
            first_entry.write("-" * 50)
            first_entry.write(current_entry)
            if position[-1] in ("end", "none"):
                first_entry.write("-" * 50)
                first_entry.write("\n")
        return -2
    return -1

def mark_deleted(to_delete: int) -> Generator[bool, None, None]:
    with Database(working_dict) as mark_d:
        if not get_total():
            yield False
        else:
            to_delete_count = mark_d.read_db("SELECT artist FROM 'music data' WHERE count = {0}".format(to_delete), as_row=True)
            if dict(next(to_delete_count))["artist"]:

                mark_d.write_db("UPDATE 'music data' SET title = '{0}', genre = '{1}', path = '{2}' || path "
                            "WHERE count = {3}".format(marked_as_d["title"],
                                                        marked_as_d["genre"], "deleted_path:", to_delete))
                yield True
            else:
                yield False

# first it yields every search result for possible deletion
# the it yields a lis  with the row count for deletion
def search_for_deletion(artist: str) -> Generator[str | list, None, None]:
    with Database(working_dict) as get_mark:
        if not get_total():
            yield []
        to_delete_artist = get_mark.read_db("SELECT * FROM 'music data' "
                                            "WHERE artist = '{0}' "
                                            "AND title != '{1}' AND genre != '{2}'".format(artist, 
                                                                                           marked_as_d["title"], 
                                                                                           marked_as_d["genre"]), as_row=True)

        count_for_delete: list = []
        for count, val in enumerate(to_delete_artist, start=1):
            yield f"{count}. [{dict(val)['count']}] {Path(dict(val)['path']).parts[-3:]}"
            count_for_delete.append(dict(val)["count"])
        yield count_for_delete

def write_metadata(file: Path, current_entry: int = 1) -> None:
    try:
        song_data: FileType | None = File(file, easy=True)
        if song_data is not None:
            labels = ("title", "artist",
                      "album", "albumartist",
                      "genre")
            # song data returns each property inside a list in case multiple values
            # are returned
            file_info = list(map(lambda x: song_data.get(x, None), labels))
            file_info: list[list[str]] = [value if value else ["unknown__value",] for value in file_info]
            if file_info[0] == ["unknown__value",]:
                file_info[0] = [str(file.stem),]
            if file_info[3] == ["unknown__value",] and file_info[1] != file_info[3]:
                file_info[3] = file_info[1]
            list_info: list[str] = [val[0] for val in file_info]
            list_info = [value if "|" not in value else value.replace("|", "/") for value in list_info]
            file_repr: list[int | str] = [current_entry]
            file_repr.extend([
                f"{value_label.lower()}"
                for value_label in list_info])
            file_repr.append(f"{song_data.info.length}")
            file_repr.append(str(file))
        else:
            print(str(file))
            sys.exit(0)
    except MutagenError as error:
        print(error, str(file))
        sys.exit(0)
        
    else:
        with open("song_data.csv", "a", newline="", encoding="utf-8") as csv_obj:
            writer = csv.writer(csv_obj, delimiter="|")
            writer.writerow(file_repr)
        ext = file.suffix.lower()
        if ext not in new_dict:
            new_dict[ext] = 1
        else:
            new_dict[ext] += 1


"""WARNING NON ASCII CHARACTER ARE NOT CASE INSENSITIVE BY DEAFAULT ON SQLITE
THIS COULD BYPASS THE FILTER FOR CERTAIN SONGS"""


def check_if_similar() -> Generator[str, None, None]:
    with open("song_data.csv", "r", newline="", encoding="utf-8") as csv_db:
        reader_init = csv.DictReader(csv_db, delimiter="|")
        count_line = 0
        for row in reader_init:
            current_value = [value for value in row.values()]
            current_fails: list = []
            with Database(working_dict) as checking:
                for possible in checking.read_db(("SELECT * FROM 'music data' "
                                                  "WHERE (title LIKE ? AND (artist LIKE ? OR albumartist LIKE ?)) "
                                                  "OR path LIKE (? OR ?) OR title = ?",
                                                  (f"%{current_value[1]}%", f"%{current_value[2]}%",
                                                   f"%{current_value[4]}%",
                                                   f"%{str(Path(current_value[-1]).stem).lower()}%",
                                                   f"%{current_value[1]}%", current_value[1])), as_row=True):
                    current_fails.append("\n".join([f"{key}: {value}" for key, value in dict(possible).items()]))
                if current_fails:
                    current_song = '\n'.join([f'{key}: {value}' for key, value in row.items()])
                    possible_copies = '\n\n'.join(current_fails)
                    count_line += 1
                    time.sleep(0.2)
                    yield (
                    f"\n{'-'*30}"
                    "\nCurrent Song:\n"
                    f"\n{current_song}"
                    "\nPossible copies:\n"
                    f"\n{possible_copies}"
                    f"\n{'-'*30}"
                    )
        yield f"\nTotal entries found with possible copies: {count_line:,}\n"
    

def check_in_current() -> Generator[str | int, None, None]:
    current_data: pd.DataFrame = pd.read_csv("song_data.csv", delimiter="|")
    current_matches: int = 0
    for data in current_data.itertuples(index=False):
        is_present = current_data["title"].str.contains(re.escape(data[1]))
        for index, present in is_present.items():
            if present and current_data.loc[index, "path"] != data[-1] and str(current_data.loc[index, "artist"]) in data[2]:
                current_matches += 1
                # does not matter if it breaks since only paths should be allowed
                # in the path column and all of them are in a same folder
                # with a path consisting of 8 components
                path_info_copy = Path(current_data.loc[index, 'path']).parts[-2:]
                path_info_og = Path(data[-1]).parts[-2:]
                yield (f"\nEntry **{data[1]}** at {path_info_og}\nmay be present in "
                f"current music to update as **{current_data.loc[index, 'title']}**\nat {path_info_copy}")
    yield current_matches
    

def get_total() -> int:
    with Database(working_dict) as total:
        return next(total.read_db(entries="SELECT MAX(count) FROM 'music data'"))[0]
    
def return_deleted() -> Generator[int, None, None]:
    with Database(working_dict) as deleted_entries:
        for marked in deleted_entries.read_db(("SELECT count FROM 'music data' WHERE title = ? AND genre = ?",
                                               [marked_as_d["title"], marked_as_d["genre"]]), as_row=True):
            yield dict(marked)["count"]
        yield 0


# MUST BE RUN AFTER GET FILES
# NO IDEA HOW TO UPDATE FILES FROM FINAL PLACE
# DB IS OUT OF SYNC BY 8 SONG NOT PASSED
# implement deleted entries options
# give option to pass multiple values from txt file
# update this to accept range of values
def update_db(start_from: int) -> list[int] | None:
    # ask for the marked deleted first then iterate 
    # the ids of each one an use it for the mark deleted
    changelog = []
    current_marked = iter([entry for entry in return_deleted()])
    which_mark = next(current_marked)
    total_count: int = get_total() if start_from else 0
    with open("song_data.csv", "r", newline="", encoding="utf-8") as csv_to_db:
        reader_init_db = csv.DictReader(csv_to_db, delimiter="|")
        for row in reader_init_db:
            current_value = [value for value in row.values()]
            current_update = '\n'.join([f"{key}: {val}" for key, val in row.items()])
            # this is for when you use update from
            if total_count and start_from <= total_count:
                current_value.append(start_from)
                with Database(working_dict) as updating:
                    change = next(updating.read_db("SELECT * FROM 'music data' WHERE count = {0}".format(start_from),
                                                   as_row=True))
                    changelog.append(dict(change)["count"])
                    updating.write_db(("UPDATE 'music data' SET title = ?, artist = ?, "
                                       "album = ?, albumartist = ?, genre = ?, "
                                       "length_seconds = ?, path = ? WHERE count = ? ", current_value[1:]))
                    start_from += 1
            else:
                if which_mark:
                    current_value.append(which_mark)
                    with Database(working_dict) as updating:
                        updating.write_db(("UPDATE 'music data' SET title = ?, artist = ?, "
                                           "album = ?, albumartist = ?, genre = ?, "
                                           "length_seconds = ?, path = ? WHERE count = ? ", current_value[1:]))
                    which_mark = next(current_marked)
                else:
                    with Database(working_dict) as updating:
                        updating.write_db(("INSERT INTO 'music data' (title, artist, "
                                           "album, albumartist, genre, length_seconds, path) "
                                           "VALUES (?, ?, ?, ?, ?, ?, ?)", current_value[1:]))
            print(f"\nUpdating:\n{current_update}\n")
    if changelog:
        return changelog

if __name__ == "__main__":
    new_dict: dict = {}
    marked_as_d: dict = {"title": "removed_entry_free_space", "genre": "deleted_song"}
    working_dict: str = str(fr"{Path(__file__).parent}\music.db")
    txt_log: str = str(fr"{Path(__file__).parent}\log\Log.txt")
    try:
        while True:
            if new_dict:
                new_dict.clear()
            str_path: str = input("Working directory or file: ").strip()
            if str_path.lower() == "n":
                break
            str_path = check_copy_path(str_path)
            working_dir: Path = Path(str_path if str_path else "n")
            with open("song_data.csv", "w", newline="", encoding="utf-8") as csv_start:
                writer_init = csv.writer(csv_start, delimiter="|")
                writer_init.writerow(("count", "title", "artist", "album",
                                      "albumartist", "genre", "length_seconds", "path"))
            if working_dir.is_dir():
                get_files(working_dir, 1)
            elif working_dir.is_file():
                file_ext: str = working_dir.suffix.lower()
                if file_ext == ".txt":
                    # use this encoding to deal with BOM
                    # (byte order mark, can show in files created with powershell or others)
                    current_count = 1
                    with open(working_dir, "r", encoding="utf-8-sig") as paths:
                        for line in paths:
                            new_path = Path(check_copy_path(line.strip("\n")))
                            if new_path.is_file():
                                write_metadata(new_path, current_count)
                                current_count += 1
                            elif new_path.is_dir():
                                current_count = get_files(new_path, current_count)
                            else:
                                print(f"Unexpected error for {str(new_path)}, it is not a valid directory or filename")
                    # see if it can be erased when updated
                elif file_ext in (".m4a", ".opus", ".flac", ".wav", ".mp3", ".ogg"):
                    write_metadata(working_dir, 1)
                else:
                    print(f"trying to update unsupported db file type: {file_ext}")
            else:
                if str(working_dir).lower() == "dlt":
                    entry_to_mark = input("Entry number or artist to delete: ").strip().lower()
                    if entry_to_mark.isnumeric() and 0 < int(entry_to_mark) <= get_total():
                        # first we attempt to log the entry before we mark it as deleted
                        logged = write_log(int(entry_to_mark))
                        if logged == 0:
                            print("\nno entries were logged cause table db is empty or entry to log was not found\n")
                        elif logged > 0:
                            print(f"\nunable to log entry number {logged} please log manually if posible (entry not deleted)\n")
                        else:
                            if next(mark_deleted(int(entry_to_mark))):
                                print(f"entry number {entry_to_mark} deleted successfully\n")
                            else:
                                print(f"\nentry number {entry_to_mark} couldn't be deleted but {'was logged' if logged < 0 else 'was not logged'}")
                    else:
                        last_item = []
                        for result in search_for_deletion(entry_to_mark):
                            if not isinstance(result, list):
                                print(result)
                            else:
                                last_item.append(result)
                        if last_item and len(last_item[0]):
                            confirm_deletion = input(f"confirm the deletion of the results of the search? (yes to confirm): ").strip().lower()
                            if confirm_deletion == "yes":
                                places: dict[int, str] = {0: "start", len(last_item[0])-1: "end"}
                                for item in range(len(last_item[0])):
                                    
                                    was_logged = write_log(last_item[0][item], position=(item + 1, places.get(item, "middle")))
                                    if was_logged == 0:
                                        print("\no entries were logged cause table db is empty or entry to log was not found\n")
                                    elif was_logged > 0:
                                        print(f"\nunable to log entry number {was_logged} please log manually if posible (entry not deleted)\n")
                                    else:
                                        if next(mark_deleted(last_item[0][item])):
                                            print(f"entry number {item + 1} deleted successfully\n")
                                        else:
                                            print(f"\nentry number {item + 1} couldn't be deleted but {'was logged' if was_logged < 0 else 'was not logged'}")
                        else:
                            print(f"no search result for artist of name {entry_to_mark} were found\n")
                    continue
                            
                elif "n" == str(working_dir).lower():
                    print("closing program")
                    sys.exit(0)
                else:
                    print(f"Unexpected error for {str(working_dir)}, it is not a valid directory or filename")
                    continue
        
            current_checked = []
            for internal_match in check_in_current():
                if not isinstance(internal_match, int):
                    print(internal_match)
                else:
                    current_checked.append(internal_match)
            if current_checked and isinstance(current_checked[0], int) and current_checked[0] > 0:
                proceed: str = input("\nProceed to db check despite the matches? (y or n): ").lower().strip()
                if proceed[0] == "n":
                    continue
            for db_match in check_if_similar():
                print(db_match)
            ask_update = input("Update to db (yes to confirm): ").lower().strip()
            if ask_update == "yes":
                ask_start_point = input("Rewrite entries from: (number to start or "
                                        "anything else to skip): ").strip()
                updated: list[int] | None = update_db(0 if not ask_start_point.isnumeric() else int(ask_start_point))
                if updated is not None:
                    for count, entry in enumerate(updated):
                        write_log(entry, position=(count+1, "start" if not count else ("middle" if count+1 != len(updated) else "end")))
                print(f"current number of entries: {get_total():,}")
                if working_dir.suffix.lower() == ".txt":
                    with open(working_dir, "w", encoding="utf-8-sig") as _:
                        pass
            if new_dict:
                print("File extensions and values:\n", *[f"{key}: {value}" for key, value in new_dict.items()])
            
    except (KeyboardInterrupt, OSError, DatabaseError, EOFError) as e:
        if isinstance(e, (OSError, DatabaseError)):
            print(e)
        sys.exit(0)
