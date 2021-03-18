import os
from pathlib import Path
import shelve
from shelve import Shelf, DbfilenameShelf

data = {'a': 0, 'b': 1, 'c': 'c-string'}

filename = str(Path.home() / "shelf")
#os.remove(filename + ".db")
db = DbfilenameShelf(filename, flag='c', protocol=3, writeback=True)
#db.update(data)
#db.sync()
print(f"shelf: {dict(db)}")
