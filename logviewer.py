import threading
import time
from typing import Any

import wx
import wx.dataview as dv


class LogView(dv.DataViewIndexListModel):

    def __init__(self, data: list[Any]):
        super().__init__(len(data))
        self.data = data

    def GetColumnType(self, col: int):
        return "string"

    def GetValueByRow(self, row: int, col: int):
        return self.data[row][col]

    def SetValueByRow(self, value: Any, row: int, col: int):
        self.data[row][col] = value
        return True

    def GetColumnCount(self):
        return len(self.data[0])

    def GetCount(self):
        return len(self.data)

    def GetAttrByRow(self, row: int, col: int, attr: dv.DataViewItemAttr):
        return False

    def Compare(self, item1: dv.DataViewItem, item2: dv.DataViewItem, col: int, ascending: bool):
        if not ascending:
            item2, item1 = item1, item2
        row1 = self.GetRow(item1)
        row2 = self.GetRow(item2)
        v = self.data[row1][col]
        w = self.data[row2][col]
        if v < w:
            return -1
        if v > w:
            return 1
        return 0

    def DeleteRows(self, rows: list[int]):
        rows = sorted(rows, reverse=True)
        for row in rows:
            del self.data[row]
            self.RowDeleted(row)

    def AddRow(self, value: Any):
        self.data.append(value)
        self.RowAppended()


class CustomStatusBar(wx.StatusBar):

    def __init__(self, parent):
        wx.StatusBar.__init__(self, parent)

        self.SetFieldsCount(3)
        self.SetStatusWidths([-2, -1, 150])
        self.sizeChanged = False
        self.Bind(wx.EVT_SIZE, self.OnSize)
        self.Bind(wx.EVT_IDLE, self.OnIdle)

        self.gauge = wx.Gauge(self)
        self.Reposition()

    def OnSize(self, evt):
        evt.Skip()
        self.Reposition()
        self.sizeChanged = True

    def OnIdle(self, evt):
        if self.sizeChanged:
            self.Reposition()

    def Reposition(self):
        rect: wx.Rect = self.GetFieldRect(2)
        rect.x += 4
        rect.y += 2
        rect.width -= 8
        rect.height -= 4
        self.gauge.SetRect(rect)
        self.sizeChanged = False


class AppFrame(wx.Frame):

    def __init__(self, parent, *args, **kw):
        super().__init__(parent, *args, **kw)
        self.logview = LogView([])
        self.dvc = self.CreateDVC(self)
        self.dvc.AssociateModel(self.logview)
        self.sbar = CustomStatusBar(self)
        self.SetStatusBar(self.sbar)
        self.drop = MyFileDropTarget(self)
        self.SetDropTarget(self.drop)
        self.Bind(wx.EVT_CLOSE, self.OnClose)

    def CreateDVC(self, parent):
        dvc = dv.DataViewCtrl(parent, style=wx.BORDER_THEME)
        dvc.AppendTextColumn("Date/Time", 0, width=150)
        dvc.AppendTextColumn("Stream", 1, width=60)
        dvc.AppendTextColumn("Layer", 2, width=50)  # CAN, TCP/IP, DoIP, UDS
        dvc.AppendTextColumn("Severity", 3, width=60)  # Error, Warning, Info, Debug
        dvc.AppendTextColumn("Event Type", 4, width=80)
        dvc.AppendTextColumn("Message", 5, width=200)
        dvc.Bind(dv.EVT_DATAVIEW_COLUMN_HEADER_RIGHT_CLICK, self.OnColumnHeaderRightClick)
        dvc.Bind(wx.EVT_CHAR, self.OnChar)
        return dvc

    def OnColumnHeaderRightClick(self, evt: dv.DataViewEvent):
        pass

    def OnClose(self, evt):
        self.drop.Abort()
        self.Destroy()

    def OnChar(self, evt: wx.KeyEvent):
        key = evt.GetKeyCode()
        if key == wx.WXK_ESCAPE:
            self.drop.Abort()
        else:
            evt.Skip()

    def SetProgressAfter(self, value):
        wx.CallAfter(self.sbar.gauge.SetValue, value)

    def LogAppendedAfter(self):
        wx.CallAfter(self.logview.RowAppended)

    def LogResetAfter(self, count):
        wx.CallAfter(self.logview.Reset, count)


class MyFileDropTarget(wx.FileDropTarget):

    def __init__(self, window: AppFrame):
        super().__init__()
        self.window = window
        self.th = None
        self.abort = False

    def OnDropFiles(self, x, y, filenames):
        self.Abort()
        self.th = threading.Thread(target=self.Process, args=(filenames,))
        self.th.start()
        self.window.sbar.SetStatusText("Press ESC to abort")
        return True

    def Abort(self):
        if self.th is not None:
            self.abort = True
            self.th.join()
            self.th = None
            self.abort = False

    def Process(self, filenames):
        progress = self.window.SetProgressAfter
        appended = self.window.LogAppendedAfter
        data = self.window.logview.data
        data.clear()
        self.window.LogResetAfter(0)
        for i in range(100):
            if self.abort:
                wx.CallAfter(self.window.sbar.SetStatusText, "")
                return
            data.append(["2020-02-03 12:20:30.334455", "ABC/DEF", "TCP", "Info", "Message", "Message Trigger"])
            progress(i * 100 // 100)
            appended()
            time.sleep(0.2)
        progress(100)
        time.sleep(1)
        progress(0)
        wx.CallAfter(self.window.sbar.SetStatusText, "")


def main():
    app = wx.App()
    frame = AppFrame(None, title="LogViewer", size=(1200, 800))
    frame.Show()
    app.MainLoop()


if __name__ == "__main__":
    main()
