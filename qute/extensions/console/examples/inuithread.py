#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import qute
from qute.extensions.console.console import PythonConsole
from qute.extensions.console.highlighter import format as format_



def greet():
    print("hello world")


if __name__ == '__main__':
    
    app = qute.QApplication([])

    console = PythonConsole(formats={
        'keyword': format_('darkBlue', 'bold')
    })
    console.push_local_ns('greet', greet)
    console.show()
    console.eval_queued()
    sys.exit(app.exec())
