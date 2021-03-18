import builtins
import json
from numbers import Number
from pathlib import Path
import platform
import sys
from typing import (
    ClassVar,
    Mapping,
    Optional,
    Sequence,
    Union,
)
from qute import QApplication, QObject

import pyqtconsole.console
from pyqtconsole import highlighter as hl


ColorLike = Union[str, Sequence[Number]]


def to_rgba(
    c: ColorLike,
    alpha: Optional[Number] = None,
    ) -> np.ndarray:

    out = mpl.colors.to_rgba(c, alpha=alpha)
    out = np.array(out) if isinstance(out, tuple) else out
    return out



def to_rgb(
    c: ColorLike,
    alpha: Optional[Number] = None,
    ) -> np.ndarray:

    out = to_rgba(c, alpha)
    out = out[..., 0:3]
    return out


def to_RGBA(
    c: ColorLike,
    alpha: Optional[Number] = None,
    ) -> np.ndarray:

    out = to_rgba(c, alpha)
    out = (255 * out).astype(np.uint8)
    return out


def to_RGB(
    c: ColorLike,
    alpha: Optional[Number] = None,
    ) -> Union[np.ndarray]:

    out = to_RGBA(c, alpha)[..., 0:3]
    return out



class Style:


    def __init__(self):
        self.path = Path(__file__).parent.parent / 'resources' / 'style'

        # Load 'qstyle.qss'.
        f = QFile(str(self.path / 'qstyle.qss'))
        f.open(QFile.ReadOnly | QFile.Text)
        self._style_sheet = QTextStream(f).readAll()

        if platform.system().lower() == 'darwin':
            mac_fix = '''
            QDockWidget::title
            {
                background-color: #31363b;
                text-align: center;
                height: 12px;
            }
            '''
            self._style_sheet += mac_fix

        # If theme has a colors.json file, read it. ??
        self.colors = {}
        with open(str(self.path / 'colors.json'), 'r') as f:
            self.colors = json.load(f)


    @property
    def style_sheet(self):
        return self._style_sheet

    def load_pixmap(self, name):
        return QPixmap(str(self.path / name))


"""
--------------------------------------------------------------------------------
"""

import enum

class ApplicationState(enum.IntEnum):
    NONE = 0
    CREATED = 1
    READY = 2
    EXECUTING = 3
    FINISHED = 4
    

_APP: Optional["Application"] = None


class Application(QObject):

    """
    Creates a QApplication instance when the singleton instance
    is initialized.

    Manages execution and cleanup of qt, including gevent integration
    for interactive consoles. The cleanup allows one to start and run
    a qt applications several times in the same session by
    connecting QApplication.aboutToQuit to QApplication.deleteLater.

    """

    _state: ApplicationState = ApplicationState.NONE
    
    #: Wrapped QApplication instance.
    _qapp: Optional[QApplication] = None
    
    #:
    _qapp_exit_status: Optional[int] = None
    

    def __new__(cls, *args, **kw):
        
        global _APP
        
        if _APP is None:
            
            if QApplication.instance() is not None:
                raise RuntimeError("QApplication instance already exists")
            
            try:
                _APP = self = object.__new__(cls)
                self._state = ApplicationState.CREATED
                self._qapp = self._create_qapp(*args, **kw)
                self._qapp_exit_status = None

            except:
                _APP = None
                self._state = ApplicationState.NONE
                self._qapp = None
                self._qapp_exit_status = None
                raise
            
            return _APP
                
        else:
            # Restart qapp if necessary.
            pass
            
            
        return _APP



    def _init(cls, *args, **kw) -> None:
        pass
        
        
    def _reinit(cls, *args, **kw) -> None:
        pass   
    
    @classmethod
    def instance(cls) -> Optional["Application"]:
        return cls._instance


    def _create_qapp(self, *args, **kw) -> Optional[QApplication]:
        if QApplication.instance() is not None:
            raise RuntimeError("QApplication instance already exists")
        self._qapp = QApplication(*args)
        self._qapp.aboutToQuit.connect(self._qapp.deleteLater)
        return self._qapp
    
    
    def _require_qapp(self):
        pass
    

    def exec(self) -> None:

        self.init_app()
        
        if self._state == ApplicationState.EXECUTING:
            return
            
        
        try:
            self._qapp.exec()
            error = None
        except Exception as e:
            error = e

        self.cleanup(error)


    def cleanup(self, error: Optional[Exception] = None) -> None:
        self._app = None
        self.init_app()
    
    
    @classmethod
    def hard_reset(cls):
        global _INSTANCE
        if _INSTANCE is None:
            return
        _INSTANCE._app = None
        _INSTANCE._owns_app = None
        _INSTANCE._state = ApplicationState.NONE




_monokai_highlighting = {
    'keyword':    hl.format("#F92672"),
    'operator':   hl.format("#F8F8F2"),
    'brace':      hl.format("#F8F8F2"),
    'defclass':   hl.format("#F92672"),
    'string':     hl.format("#E6DB74"),
    'string2':    hl.format("#E6DB74"),
    'comment':    hl.format("#75715E"),
    'self':       hl.format("#F8F8F2"),
    'numbers':    hl.format("#AE81FF"),
    'inprompt':   hl.format("#F8F8F2"),
    'outprompt':  hl.format("#F8F8F2"),
}




class PythonConsole(pyqtconsole.console.PythonConsole):

    """Interactive python GUI console."""



    def __init__(self,
                 namespace: Optional[Mapping] = None,
                 show: bool = True,
                 new_thread: bool = False,
                 parent: Optional[QtWidgets.QWidget] = None,
                 ):

        super().__init__(parent, namespace, _monokai_highlighting)


        # Modify font and background colors.
        self.setStyleSheet(
            f"""
            QPlainTextEdit {{
                background-color: #272822;
                color: #F8F8F2;
                font-family: Monaco;
                font-size: 12px;
                font-weight: 63;
            }}
            """
        )


        # Add highlighting for built-ins.
        python_builtins = dir(builtins)
        builtin_style = hl.format("#66D9EF")
        builtin_rules = [(QtCore.QRegExp(r'\b%s\b' % w), 0, builtin_style) \
                         for w in python_builtins]
        self.highlighter.rules.extend(builtin_rules)


        # Prepare console to work in main UI thread.
        if new_thread:
            self.eval_in_thread()
        else:
            self.eval_queued()

        if show:
            self.show()


    def sizeHint(self) -> QSize:
        s = QSize(600, 450)
        return s



def open_console():
    app = Application()
    c = PythonConsole()
    app.exec()
    return c



