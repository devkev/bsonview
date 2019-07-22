/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

//#include <boost/filesystem/operations.hpp>
//#include <cctype>
#include <cerrno>
//#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
//#include <pcrecpp.h>
//#include <signal.h>
//#include <stdio.h>
//#include <string.h>
#include <unistd.h>
#include <sys/mman.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/matcher/matcher.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/errno_util.h"
#include "mongo/util/quick_exit.h"

#include <tickit.h>

using namespace std::literals::string_literals;
using namespace mongo;

namespace mongo {
enum ShellExitCode : int {
    kDBException = 1,
    kInputFileError = -3,
    kTermError = -4,
    //kMongorcError = -5,
    //kUnterminatedProcess = -6,
    //kProcessTerminationError = -7,
};
}

void noop() {}

class BSONCache {

public:
    BSONCache()
    : _base(nullptr), _end(nullptr), _complete(false)
    {
    }

    BSONCache(const char* base, const char* end)
    : _base(base), _end(end), _complete(false)
    {
        _docs.push_back(BSONObj(base));
    }

    void init(const char* base, const char* end) {
        _base = base;
        _end = end;
        _complete = false;
        _docs.empty();
        _docs.push_back(BSONObj(base));
    }

    const BSONObj& operator[](unsigned long index) {
        _loadTo(index);
        return _docs[index];
    }

    bool isComplete() const {
        return _complete;
    }

    unsigned long numDocs() const {
        return _docs.size();
    }

    void loadAll(std::function<void(void)> cb = noop) {
        unsigned long i = 0;
        while ( ! isComplete()) {
            _loadNext();
            if (i % 1000 == 0) {
                cb();
            }
            i++;
        }
    }

    // TODO: convert the limit to be a Duration
    void loadSome(unsigned long maxDocs = 100) {
        unsigned long i = 0;
        while ( ! isComplete() && i < maxDocs) {
            _loadNext();
            i++;
        }
    }

    size_t sizeOfFile() const {
        return _getEnd() - _getBase();
    }

    size_t sizeOfFileSeen() const {
        return _getNextBase() - _getBase();
    }

    double percOfFileSeen() const {
        return ((double)sizeOfFileSeen()) / ((double)sizeOfFile()) * 100.0;
    }

private:

    void _loadTo(unsigned long index) {
        while (index >= _docs.size()) {
            _loadNext();
        }
    }

    const BSONObj& _getLast() const {
        return _docs.back();
    }

    const BSONObj& _getFirst() const {
        return _docs.front();
    }

    const char* _getBase() const {
        return _base;
    }

    const char* _getEnd() const {
        return _end;
    }

    const char* _getNextBase() const {
        auto last = _getLast();
        return last.objdata() + last.objsize();
    }

    void _loadNext() {
        if ( ! isComplete()) {
            auto nextBase = _getNextBase();
            // TODO: catch bson exceptions and don't abort the whole program on them
            _docs.push_back(BSONObj(nextBase));

            nextBase = _getNextBase();
            if (nextBase >= _getEnd()) {
                _complete = true;
            }
        }
    }

    std::vector<BSONObj> _docs;
    const char* _base;
    const char* _end;
    bool _complete;
};



const char* infname = nullptr;


Tickit *t = nullptr;
TickitWindow *root = nullptr;
TickitWindow *mainwin = nullptr;

bool jumpToEndAfterLoadingComplete;


static bool isKey(TickitKeyEventInfo* ev, char ch) {
    return (ev->type == TICKIT_KEYEV_TEXT && ev->str[0] == ch);
}

static bool isKey(TickitKeyEventInfo* ev, const char* str) {
    return (ev->type == TICKIT_KEYEV_KEY && strcmp(ev->str, str) == 0);
}


static TickitPen *mkpen_highlight(void) {
    static TickitPen *pen;
    if (!pen) {
        pen = tickit_pen_new_attrs(
                                   TICKIT_PEN_REVERSE, 1,
                                   0);
    }

    return pen;
}

static TickitPen *mkpen_cursorLine(void) {
    static TickitPen *pen;
    if (!pen) {
        pen = tickit_pen_new_attrs(
                                   TICKIT_PEN_FG,   0, // black
                                   TICKIT_PEN_BG,   3, // yellow
                                   0);
    }

    return pen;
}

static TickitPen *mkpen_markedDoc(void) {
    static TickitPen *pen;
    if (!pen) {
        pen = tickit_pen_new_attrs(
                                   TICKIT_PEN_FG,   0, // black
                                   TICKIT_PEN_BG,   4+8, // hi-blue
                                   0);
    }

    return pen;
}

static TickitPen *mkpen_matchedDoc(void) {
    static TickitPen *pen;
    if (!pen) {
        pen = tickit_pen_new_attrs(
                                   TICKIT_PEN_FG,   0, // black
                                   TICKIT_PEN_BG,   2+8, // hi-green
                                   0);
    }

    return pen;
}

static TickitPen *mkpen_base(void) {
    static TickitPen *pen;
    if (!pen) {
        pen = tickit_pen_new_attrs(
                                   TICKIT_PEN_FG,   7, // white
                                   TICKIT_PEN_BG,   0, // black
                                   TICKIT_PEN_BOLD,   0, // black
                                   0);
    }

    return pen;
}


class SingleLinePrompt {
public:
    SingleLinePrompt() {
    }

    SingleLinePrompt(TickitWindow* parent, TickitWindow* returnFocusTo = nullptr, int line = -1) {
        init(parent, returnFocusTo, line);
    }

    void init(TickitWindow* parent, TickitWindow* returnFocusTo = nullptr, int line = -1) {
        _parent = parent;
        _returnFocusTo = returnFocusTo;
        _line = line;

        _win = tickit_window_new(root, (TickitRect){ .top = ( (_line >= 0) ? _line : tickit_window_lines(_parent) - (-_line) ), .left = 0, .lines = 1, .cols = tickit_window_cols(_parent) }, (TickitWindowFlags)(TICKIT_WINDOW_HIDDEN));
        tickit_window_set_cursor_visible(_win, true);

        _promptPen = mkpen_highlight();
        _inputPen = mkpen_base();

        tickit_window_bind_event(_win, TICKIT_WINDOW_ON_EXPOSE, (TickitBindFlags)0, &_render_cb, this);
        tickit_window_bind_event(_win, TICKIT_WINDOW_ON_KEY, (TickitBindFlags)0, &_event_key_cb, this);
    }

    void setPrompt(const std::string& s) {
        _prompt = s;
    }

    void setEnteredText(const std::string& s) {
        _enteredText = s;
        _cursorCol = _enteredText.length();
    }

    void setCallbacks(std::function<void(const std::string&)> confirm_cb, std::function<void(const std::string&)> cancel_cb = _noop) {
        _confirm_cb = confirm_cb;
        _cancel_cb = cancel_cb;
    }

    void enter(const std::string& prompt, const std::string& initialEnteredText, std::function<void(const std::string&)> confirm_cb, std::function<void(const std::string&)> cancel_cb = _noop) {
        setPrompt(prompt);
        setEnteredText(initialEnteredText);
        setCallbacks(confirm_cb, cancel_cb);

        tickit_window_raise_to_front(_win);
        tickit_window_show(_win);
        tickit_window_take_focus(_win);
    }

    void exit() {
        tickit_window_hide(_win);
        if (_returnFocusTo) {
            tickit_window_take_focus(_returnFocusTo);
        }
    }

    void resize() {
        tickit_window_set_geometry(_win, (TickitRect){ .top = ( (_line >= 0) ? _line : tickit_window_lines(_parent) - _line ), .left = 0, .lines = 1, .cols = tickit_window_cols(_parent) });
    }

    void expose() {
        tickit_window_expose(_win, NULL);
    }


private:
    static void _noop(std::string) { }

    static int _render_cb(TickitWindow *win, TickitEventFlags flags, void *_info, void *data) {
        SingleLinePrompt* prompt = static_cast<SingleLinePrompt*>(data);
        if (prompt) {
            return prompt->_render(win, flags, _info);
        }
        return 1;
    }

    int _render(TickitWindow *win, TickitEventFlags flags, void *_info) {
        TickitExposeEventInfo *info = static_cast<TickitExposeEventInfo*>(_info);
        TickitRenderBuffer *rb = info->rb;

        tickit_renderbuffer_clear(rb);

        tickit_renderbuffer_setpen(rb, _promptPen);
        tickit_renderbuffer_text_at(rb, 0, 0, _prompt.c_str());

        tickit_renderbuffer_setpen(rb, _inputPen);
        tickit_renderbuffer_text_at(rb, 0, _prompt.size(), _enteredText.c_str());

        tickit_window_set_cursor_position(win, 0, _prompt.size() + _cursorCol);

        return 1;
    }

    static int _event_key_cb(TickitWindow *win, TickitEventFlags flags, void *_info, void *data) {
        SingleLinePrompt* prompt = static_cast<SingleLinePrompt*>(data);
        if (prompt) {
            return prompt->_event_key(win, flags, _info);
        }
        return 0;
    }

    int _event_key(TickitWindow *win, TickitEventFlags flags, void *_info) {
        TickitKeyEventInfo *info = static_cast<TickitKeyEventInfo*>(_info);

        if ( ! info->str) {
            return 1;
        }

        if (info->type == TICKIT_KEYEV_TEXT ) {
            _enteredText += info->str;
            _cursorCol++;
            expose();

        } else if (isKey(info, "Backspace")) {
            if (_cursorCol > 0) {
                _enteredText = _enteredText.substr(0, _cursorCol - 1) + _enteredText.substr(_cursorCol);
                _cursorCol--;
                expose();
            } else if (_enteredText == "") {
                exit();
                _doCallback(_cancel_cb);
            }

        } else if (isKey(info, "Delete")) {
            if (_cursorCol < (int)_enteredText.length()) {
                _enteredText = _enteredText.substr(0, _cursorCol) + _enteredText.substr(_cursorCol + 1);
                expose();
            }

        } else if (isKey(info, "Left")) {
            if (_cursorCol > 0) {
                _cursorCol--;
                expose();
            }

        } else if (isKey(info, "Right")) {
            if (_cursorCol < (int)_enteredText.length()) {
                _cursorCol++;
                expose();
            }

        } else if (isKey(info, "Home") || isKey(info, "C-a")) {
            if (_cursorCol != 0) {
                _cursorCol = 0;
                expose();
            }

        } else if (isKey(info, "End") || isKey(info, "C-e")) {
            if (_cursorCol != (int)_enteredText.length()) {
                _cursorCol = (int)_enteredText.length();
                expose();
            }

        } else if (isKey(info, "C-u")) {
            if (_cursorCol > 0) {
                // TODO: save it somewhere to restore with C-y
                _enteredText = _enteredText.substr(_cursorCol);
                _cursorCol = 0;
                expose();
            }

        } else if (isKey(info, "Up")) {
            // TODO: needs callbacks

        } else if (isKey(info, "Down")) {
            // TODO: needs callbacks

        } else if (isKey(info, "Escape")) {
            exit();
            _doCallback(_cancel_cb);

        } else if (isKey(info, "Enter")) {
            exit();
            _doCallback(_confirm_cb);

        }

        return 1;
    }

    void _doCallback(std::function<void(const std::string&)> cb) {
        if (cb) {
            cb(_enteredText);
        }
    }


    TickitWindow* _parent;
    TickitWindow* _win = nullptr;
    TickitWindow* _returnFocusTo = nullptr;
    int _line;
    TickitPen* _promptPen;
    TickitPen* _inputPen;

    std::string _prompt;
    std::string _enteredText;
    std::function<void(const std::string&)> _confirm_cb = _noop;
    std::function<void(const std::string&)> _cancel_cb = _noop;
    int _cursorCol;

};


class BSONCacheView;

class Search {
public:
    Search(const std::string& s);
    virtual ~Search();

    virtual bool matches(unsigned long doc, BSONCacheView& view) const = 0;

    virtual bool isValid() const = 0;

protected:
    const std::string& getText() const;

private:
    std::string _text;
};


class SearchRenderedText : public Search {
public:
    SearchRenderedText(const std::string& s);
    virtual ~SearchRenderedText();

    virtual bool matches(unsigned long doc, BSONCacheView& view) const;

    virtual bool isValid() const;

};


class SearchMQL : public Search {
public:
    SearchMQL(const std::string& s);
    virtual ~SearchMQL();

    virtual bool matches(unsigned long doc, BSONCacheView& view) const;

    virtual bool isValid() const;

private:
    BSONObj _pattern;
    static const boost::intrusive_ptr<ExpressionContext> _expCtx;
    Matcher* _matcher;
    bool _valid;
};

const boost::intrusive_ptr<ExpressionContext> SearchMQL::_expCtx = new ExpressionContext(nullptr, nullptr);



std::string textLogs(const BSONObj& doc) {
    // TODO: this code is foul
    StringBuilder sb;
    int i = 1;
    for (auto&& elem : doc) {
        if (i == 1) {
            sb << elem.Date().toString();
        } else if (i == 2) {
            char c = toupper(elem.String()[0]);
            sb << " " << c;
        } else if (i == 3) {
            sb << " " << elem.String();
        } else if (i == 4) {
            sb << " [" << elem.String() << "]";
        } else if (i == 5) {
            auto msg = elem.String();
            while (msg.length() > 0 && msg[msg.length()-1] == '\n') {
                msg = msg.substr(0, msg.length()-1);
            }
            while (msg[0] == '\t') {
                msg = "        " + msg.substr(1);
            }
            sb << " " << msg;
            break;
        }
        i++;
    }
    return sb.str();
}


class BSONCacheView {
public:

    enum DocumentRenderMode {
        kJSONOneline,
        kJSONPretty,
        kToString,
        kTextLogs,
    };


    BSONCacheView(BSONCache* cache = nullptr, std::function<void(void)> redrawFullFn = noop, std::function<void(void)> redrawStatusFn = noop)
    : _cache(cache), _redrawFullFn(redrawFullFn), _redrawStatusFn(redrawStatusFn) {
    }

    void init(BSONCache* cache = nullptr, std::function<void(void)> redrawFullFn = noop, std::function<void(void)> redrawStatusFn = noop)
    {
        _cache = cache;
        _redrawFullFn = redrawFullFn;
        _redrawStatusFn = redrawStatusFn;
    }

    BSONCache& cache() {
        return *_cache;
    }

    void moveLeft() {
        if (_startCol > 0) {
            _startCol--;
            computeVisible();
            redrawFull();
        }
    }

    void moveRight() {
        if (_startCol < _longestLineStartCol) {
            _startCol++;
            computeVisible();
            redrawFull();
        }
    }

    void jumpLeft() {
        if (_startCol != 0) {
            _startCol = 0;
            computeVisible();
            redrawFull();
        }
    }

    void jumpRight() {
        int targetStartCol = (_longestLineStartCol > 0) ? _longestLineStartCol : 0;
        if (_startCol != targetStartCol) {
            _startCol = targetStartCol;
            computeVisible();
            redrawFull();
        }
    }

    void cursorTop() {
        int target = 0;
        if (_cursorLine != target) {
            _cursorLine = target;
            computeVisible();
            redrawFull();
        }
    }

    void cursorMiddle() {
        int target = _mainLines / 2;
        if (target > _lastDisplayedLine) {
            target = _lastDisplayedLine;
        }
        if (_cursorLine != target) {
            _cursorLine = target;
            computeVisible();
            redrawFull();
        }
    }

    void cursorBottom() {
        int target = _mainLines - 1;
        if (target > _lastDisplayedLine) {
            target = _lastDisplayedLine;
        }
        if (_cursorLine != target) {
            _cursorLine = target;
            computeVisible();
            redrawFull();
        }
    }

    void cursorUp() {
        if (_cursorLine > 0) {
            _cursorLine--;
            computeVisible();
            redrawFull();
        }
    }

    void moveCursorUp() {
        // push on the top of the screen to scroll up (if possible)
        if (_cursorLine == 0) {
            moveUp();
        }
        cursorUp();
    }

    void cursorDown() {
        if (_cursorLine < _mainLines - 1 && _cursorLine < _lastDisplayedLine) {
            _cursorLine++;
            computeVisible();
            redrawFull();
        }
    }

    void moveCursorDown() {
        // push on the bottom of the screen to scroll up (if possible)
        if (_cursorLine == _mainLines - 1) {
            moveDown();
        }
        cursorDown();
    }

    bool nextDoc() {
        if (!cache().isComplete() || _startDoc < cache().numDocs() - 1) {
            _startDoc++;
            _startLine = 0;
            return true;
        }
        return false;
    }

    bool prevDoc() {
        if (_startDoc > 0) {
            _startDoc--;
            _startLine = 0;
            return true;
        }
        return false;
    }

    void moveNextDoc() {
        if (nextDoc()) {
            computeVisible();
            redrawFull();
        }
    }

    void movePrevDoc() {
        if (prevDoc()) {
            computeVisible();
            redrawFull();
        }
    }

    void moveDown() {
        computeVisible();
        if (_startLine == _docLines[0] - 1) {
            if (nextDoc()) {
                _startLine = 0;
                computeVisible();
                cursorUp();
                redrawFull();
            }
        } else {
            _startLine++;
            computeVisible();
            cursorUp();
            redrawFull();
        }
    }

    void moveUp() {
        computeVisible();
        if (_startLine == 0) {
            if (prevDoc()) {
                _startLine = _docLines[0] - 1;
                computeVisible();
                cursorDown();
                redrawFull();
            }
        } else {
            _startLine--;
            computeVisible();
            cursorDown();
            redrawFull();
        }
    }

    void jumpUp() {
        if (_startDoc != 0 || _startLine != 0) {
            _startDoc = 0;
            _startLine = 0;
            computeVisible();
            redrawFull();
        }
        cursorTop();
    }

    void jumpDown() {
        if ( ! cache().isComplete()) {
            // TODO: indicate to the user that there might be a delay?
            jumpToEndAfterLoadingComplete = true;
        } else {
            unsigned long targetStartDoc = cache().numDocs() - 1;
            _startDoc = targetStartDoc;
            computeVisible();
            while (_lastDisplayedLine < _mainLines - 2) {
                _startDoc--;
                computeVisible();
            }
            auto totalLines = getTotalDocLines();
            _startLine = totalLines - (_mainLines - 2);
            computeVisible();
            redrawFull();
            cursorBottom();

            jumpToEndAfterLoadingComplete = false;
        }
    }

    void pageUp() {
        if (_startDoc == 0 && _startLine == 0) {
            // we are at the top of the first page.  cannot page up any further.
            cursorTop();
        } else {
            bool crashedIntoTop = false;
            unsigned long oldStartDoc = _startDoc;
            while (_lastDisplayedDoc != oldStartDoc) {
                if (_startDoc == 0) {
                    crashedIntoTop = true;
                    break;
                }
                _startDoc--;
                computeVisible();
            }
            if (crashedIntoTop) {
                // cursor special handling
                // the cursor has to go DOWN by as many lines as we've shifted UP
                unsigned long docNum = 0;
                for (std::vector<int>::const_iterator it = _docLines.begin(); it != _docLines.end(); ++it) {
                    if (docNum == oldStartDoc) {
                        break;
                    }
                    _cursorLine += *it;
                    docNum++;
                }
                _cursorLine += _startLine;
                if (_cursorLine > _mainLines - 1) {
                    _cursorLine = _mainLines - 1;
                }
                _startLine = 0;
            } else {
                _startLine = getTotalDocLines() - (_docLines.back() - _startLine) - _mainLines;
                cursorBottom();
            }
            computeVisible();
            redrawFull();
        }
    }

    void pageDown() {
        if (_lastDisplayedLine < _mainLines - 1) {
            // we are on the last page.  cannot page down any further.
            cursorBottom();
        } else {
            _startDoc = _lastDisplayedDoc;
            _startLine = _docLines.back() - (getTotalDocLines() - _startLine - _mainLines);
            cursorTop();
            computeVisible();
            if (_lastDisplayedDoc == cache().numDocs() - 1) {
                int emptyLines = _mainLines - 1 - _lastDisplayedLine;
                jumpDown();
                _cursorLine = emptyLines;
            }
            computeVisible();
            redrawFull();
        }
    }

    int getTotalDocLines() {
        return std::accumulate(_docLines.begin(), _docLines.end(), 0);
    }


    void setDocumentRenderMode(DocumentRenderMode documentRenderMode) {
        _documentRenderMode = documentRenderMode;
        _startCol = 0;
        // TODO: take some care to keep the cursor on the same doc, if possible / at all costs.
        computeVisible();
        redrawFull();
    }

    DocumentRenderMode getDocumentRenderMode() {
        return _documentRenderMode;
    }

    void setExtendedJSONMode(JsonStringFormat extendedJSONMode) {
        _extendedJSONMode = extendedJSONMode;
        computeVisible();
        redrawFull();
    }

    JsonStringFormat getExtendedJSONMode() {
        return _extendedJSONMode;
    }

    void toggleExtendedJSONMode() {
        if (getExtendedJSONMode() == Strict) {
            setExtendedJSONMode(TenGen);
        } else {
            setExtendedJSONMode(Strict);
        }
    }

    std::string renderDoc(unsigned long doc) {
        switch (_documentRenderMode) {
            case kJSONOneline: return cache()[doc].jsonString(_extendedJSONMode);
            case kJSONPretty:  return cache()[doc].jsonString(_extendedJSONMode, 1);
            case kToString:    return cache()[doc].toString();
            case kTextLogs:    return textLogs(cache()[doc]);
        }
        return "--- unknown render mode ---";
    }


    void updateDimensions(TickitWindow *win) {
        int new_mainLines = tickit_window_lines(win);
        int new_mainCols = tickit_window_cols(win);
        bool changed = new_mainLines != _mainLines || new_mainCols != _mainCols;
        _mainLines = new_mainLines;
        _mainCols = new_mainCols;
        if (changed) {
            computeVisible();
        }
    }


    void drawStatusBar(TickitRenderBuffer* rb) {
        tickit_renderbuffer_textf_at(rb, 0, 0, "%s [doc %ld] [docs %ld-%ld/%ld%s%s] [loaded %.0lf%% %.0lf/%.0lf MiB]", infname, _cursorDoc, _startDoc, _lastDisplayedDoc, cache().numDocs(), cache().isComplete() ? "" : "+", cache().isComplete() && _lastDisplayedDoc == cache().numDocs() - 1 ? " (END)" : "", cache().percOfFileSeen(), cache().sizeOfFileSeen()/1048576.0, cache().sizeOfFile()/1048576.0);
    }


    void computeVisible() {
        int line = 0;
        int longestLine = 0;
        unsigned long doc = _startDoc;
        _docLines.clear();
        int skipLines = _startLine;
        while (line < _mainLines && (!cache().isComplete() || doc < cache().numDocs()) ) {

            std::string str = renderDoc(doc);

            const char* ss = str.c_str();

            const char* s = ss;
            const char* e;

            // TODO: when we directly render docs (for color syntax highlighting), it should be possible to more quickly compute the number of lines each visible doc will need (for a given rendering mode)
            int thisDocLines = 0;

            while (line < _mainLines) {

                e = strchr(s, '\n');
                int len = e ? (e - s) : strlen(s);

                if (skipLines > 0) {
                    skipLines--;
                } else {

                    if (longestLine < len) {
                        longestLine = len;
                    }

                    if (line == _cursorLine) {
                        _cursorDoc = doc;
                    }

                    line++;
                }

                thisDocLines++;

                if ( ! e) {
                    // last sub-line, get out
                    break;
                }
                s = e + 1;
            }
            _docLines.push_back(thisDocLines);
            // this should no longer ever happen
            //if (line < _startLine) {
            //    // _startLine means we're skipping that whole doc
            //    view.nextDoc();
            //}
            _lastDisplayedDoc = doc;
            doc++;
        }
        _lastDisplayedLine = line - 1;
        //if (_startLine) {
        //    _lastDisplayedLine--; // bleh there are better ways to fix this (like understanding the problem properly), but this band-aid will do for now
        //}
        _longestLineStartCol = longestLine - _mainCols;
    }


    void drawMainLines(TickitRenderBuffer* rb) {
        int line = 0;
        unsigned long doc = _startDoc;
        int skipLines = _startLine;
        while (line < _mainLines && (!cache().isComplete() || doc < cache().numDocs()) ) {

            std::string str = renderDoc(doc);

            auto lastSearch = getLastSearch();
            bool docMatch = lastSearch ? (*lastSearch)->matches(doc, *this) : false;

            const char* ss = str.c_str();

            const char* s = ss;
            const char* e;

            while (line < _mainLines) {

                e = strchr(s, '\n');
                int len = e ? (e - s) : strlen(s);

                if (skipLines > 0) {
                    skipLines--;
                } else {

                    TickitPen* specialPen = nullptr;
                    if (line == _cursorLine) {
                        specialPen = mkpen_cursorLine();
                    } else if (docMatch) {
                        specialPen = mkpen_matchedDoc();
                    } else if (isMarkedDoc(doc)) {
                        specialPen = mkpen_markedDoc();
                    }
                    if (specialPen) {
                        tickit_renderbuffer_savepen(rb);
                        tickit_renderbuffer_setpen(rb, specialPen);
                        TickitRect thisLineRect{ .top = line, .left = 0, .lines = 1, .cols = _mainCols };
                        tickit_renderbuffer_eraserect(rb, &thisLineRect);
                    }

                    if (_startCol < len) {
                        tickit_renderbuffer_textn_at(rb, line, 0, s + _startCol, len - _startCol);
                    }
                    if (_startCol > 0) {
                        tickit_renderbuffer_text_at(rb, line, 0, "<");
                    }
                    if (len - _startCol > _mainCols) {
                        tickit_renderbuffer_text_at(rb, line, _mainCols - 1, ">");
                    }

                    if (specialPen) {
                        tickit_renderbuffer_restore(rb);
                    }

                    line++;
                }

                if ( ! e) {
                    // last sub-line, get out
                    break;
                }
                s = e + 1;
            }
            doc++;
        }
    }


    void drawTildeLines(TickitRenderBuffer* rb) {
        for (int line = _lastDisplayedLine + 1; line < _mainLines; line++) {
            tickit_renderbuffer_text_at(rb, line, 0, "~");
        }
    }


    void redrawFull() {
        _redrawFullFn();
    }

    void redrawStatus() {
        _redrawStatusFn();
    }



    bool isMarkedDoc(unsigned long doc) const {
        if (_dragMarked) {
            if ( (_dragFirst <= _dragLast && _dragFirst <= doc && doc <= _dragLast ) ||
                 (_dragFirst >  _dragLast && _dragLast  <= doc && doc <= _dragFirst) ) {
                return *_dragMarked;
            }
        }
        return _markedDocs.find(doc) != _markedDocs.end();
    }

    void dragStart(unsigned long doc) {
        _dragMarked = ! isMarkedDoc(doc);
        _dragFirst = _dragLast = doc;
        redrawFull();
    }

    void dragUpdate(unsigned long doc) {
        _dragLast = doc;
        redrawFull();
    }

    void dragEnd(unsigned long doc) {
        _dragLast = doc;

        // make permanent
        if (_dragFirst > _dragLast) {
            // upwards drag
            std::swap(_dragFirst, _dragLast);
        }
        for (unsigned long doc = _dragFirst; doc <= _dragLast; doc++) {
            if (*_dragMarked) {
                markDoc(doc);
            } else {
                unmarkDoc(doc);
            }
        }

        _dragMarked = boost::none;

        redrawFull();
    }

    void dragStartLine(int line) {
        auto doc = docForLine(line);
        if (doc) {
            dragStart(*doc);
        }
    }

    void dragUpdateLine(int line) {
        auto doc = docForLine(line);
        if (doc) {
            dragUpdate(*doc);
        }
    }

    void dragEndLine(int line) {
        auto doc = docForLine(line);
        if (doc) {
            dragEnd(*doc);
        }
    }

    void markDoc(unsigned long doc) {
        _markedDocs.insert(doc);
    }

    void unmarkDoc(unsigned long doc) {
        _markedDocs.erase(doc);
    }

    void toggleMarkDoc(unsigned long doc) {
        if (isMarkedDoc(doc)) {
            unmarkDoc(doc);
        } else {
            markDoc(doc);
        }
    }

    boost::optional<unsigned long> nextMarkedDoc(unsigned long doc) const {
        if (_markedDocs.size() == 0) {
            return boost::none;
        }
        const auto next = _markedDocs.upper_bound(doc);
        if (next != _markedDocs.end()) {
            return *next;
        } else {
            // wrap to front
            return *_markedDocs.begin();
        }
    }

    boost::optional<unsigned long> prevMarkedDoc(unsigned long doc) const {
        if (_markedDocs.size() == 0) {
            return boost::none;
        }
        auto nextOrThis = _markedDocs.lower_bound(doc);  // this will return the next one (or this one).  but we want the one before.
        if (nextOrThis == _markedDocs.begin()) {
            // wrap to back
            return *_markedDocs.rbegin();
        } else {
            return *(--nextOrThis);
        }
    }


    void markCursorDoc() {
        markDoc(_cursorDoc);
        redrawFull();
    }

    void unmarkCursorDoc() {
        unmarkDoc(_cursorDoc);
        redrawFull();
    }

    void toggleMarkCursorDoc() {
        toggleMarkDoc(_cursorDoc);
        redrawFull();
    }

    void jumpToDoc(unsigned long doc) {
        if (doc < _startDoc || (doc == _startDoc && _startLine > 0)) {
            // we are jumping backwards
            _jumpToDocBackwards(doc);

        } else if (doc > _lastDisplayedDoc) {
            // we are jumping forwards
            _jumpToDocForwards(doc);

        } else {
            // we are jumping to a doc which has its first line on this page.
            _jumpToDocOnscreen(doc);
        }
    }

    void jumpNextMarkedDoc() {
        auto target = nextMarkedDoc(_cursorDoc);
        if (target) {
            jumpToDoc(*target);
        }
    }

    void jumpPrevMarkedDoc() {
        auto target = prevMarkedDoc(_cursorDoc);
        if (target) {
            jumpToDoc(*target);
        }
    }

    boost::optional<unsigned long> docForLine(int line) const {
        int l = - _startLine;
        for (unsigned long doc = 0; doc < _docLines.size(); doc++) {
            int prevl = l;
            l += _docLines[doc];
            if (prevl <= line && line < l) {
                return _startDoc + doc;
            }
        }
        return boost::none;
    }

    boost::optional<bool> isMarkedDocOnLine(int line) const {
        auto doc = docForLine(line);
        if (doc) {
            return isMarkedDoc(*doc);
        }
        return boost::none;
    }

    void markDocOnLine(int line) {
        auto doc = docForLine(line);
        if (doc) {
            markDoc(*doc);
            redrawFull();
        }
    }

    void unmarkDocOnLine(int line) {
        auto doc = docForLine(line);
        if (doc) {
            unmarkDoc(*doc);
            redrawFull();
        }
    }

    void toggleMarkDocOnLine(int line) {
        auto doc = docForLine(line);
        if (doc) {
            toggleMarkDoc(*doc);
            redrawFull();
        }
    }


    boost::optional<unsigned long> searchFor(const Search* s) {
        for (unsigned long curr = _cursorDoc + 1; curr < cache().numDocs(); curr++) {
            if (s->matches(curr, *this)) {
                return curr;
            }
        }
        return boost::none;
    }

    void registerSearch(Search* s) {
        if (_lastSearch) {
            delete _lastSearch;
        }
        _lastSearch = s;
    }

    boost::optional<const Search*> getLastSearch() const {
        if (_lastSearch) {
            return _lastSearch;
        } else {
            return boost::none;
        }
    }

    unsigned long getCursorDoc() {
        return _cursorDoc;
    }

    unsigned long getStartDoc() {
        return _startDoc;
    }

    unsigned long getLastDisplayedDoc() {
        return _lastDisplayedDoc;
    }

    const MatchDetails* getMatchDetails() const {
        return &_matchDetails;
    }

    MatchDetails* getMatchDetails() {
        return &_matchDetails;
    }


private:

    void _jumpToDocOffscreen(unsigned long doc, boost::optional<int> targetLine = boost::none) {
        _startDoc = doc;
        _startLine = 0;
        _cursorLine = 0;

        if ( ! targetLine) {
            targetLine = _mainLines / 4;
        }
        if (*targetLine > 0) {
            for (int i = 0; i < *targetLine; i++) {
                moveUp();
            }
        } else {
            // happens as part of moveUp(), so need to do it if we don't call moveUp().
            computeVisible();
        }

        redrawFull();
    }

    void _jumpToDocBackwards(unsigned long doc) {
        // optimise for backwards jumping, in terms of the target line on the screen, eg. target line is 3/4 or 2/3 of the screen.
        //_jumpToDocOffscreen(doc, _mainLines * 3 / 4);   // confusing, bad
        _jumpToDocOffscreen(doc);
    }

    void _jumpToDocForwards(unsigned long doc) {
        // optimise for forwards jumping, in terms of the target line on the screen, eg. target line is 1/4 or 1/3 of the screen.
        _jumpToDocOffscreen(doc);
    }

    void _jumpToDocOnscreen(unsigned long doc) {
        _cursorLine = 0;
        _cursorLine -= _startLine;  // for partial first doc
        for (unsigned long d = _startDoc; d != doc && d < _lastDisplayedDoc; d++) {
            _cursorLine += _docLines[d - _startDoc];
        }
        computeVisible();
        redrawFull();
    }




    BSONCache* _cache;

    DocumentRenderMode _documentRenderMode = kJSONOneline;

    int _startCol = 0;
    int _longestLineStartCol = 0;

    unsigned long _startDoc = 0;   // index of first doc to display on the screen
    int _startLine = 0;            // number of lines of the _startDoc to skip displaying
    unsigned long _lastDisplayedDoc = 0;
    int _lastDisplayedLine = 0;
    std::vector<int> _docLines;

    int _cursorLine = 0;
    unsigned long _cursorDoc = 0;

    int _mainLines = 0;
    int _mainCols = 0;

    std::set<unsigned long> _markedDocs;

    boost::optional<bool> _dragMarked = boost::none;
    unsigned long _dragFirst;  // inclusive
    unsigned long _dragLast;  // inclusive

    std::function<void(void)> _redrawFullFn = noop;
    std::function<void(void)> _redrawStatusFn = noop;

    // TODO: length-limited list instead
    Search* _lastSearch = nullptr;

    JsonStringFormat _extendedJSONMode = Strict;

    MatchDetails _matchDetails;

};


Search::Search(const std::string& s)
: _text(s)
{
}

Search::~Search() {
}

const std::string& Search::getText() const {
    return _text;
}


SearchRenderedText::SearchRenderedText(const std::string& s)
: Search(s)
{
}

SearchRenderedText::~SearchRenderedText() {
}

bool SearchRenderedText::matches(unsigned long doc, BSONCacheView& view) const {
    if ( ! isValid()) {
        return false;
    }
    // TODO: ergh this is so horribly slow
    return (view.renderDoc(doc).find(getText()) != std::string::npos);
}

bool SearchRenderedText::isValid() const {
    return (getText() != "");
}


SearchMQL::SearchMQL(const std::string& s)
: Search(s), _valid(false)
{
    try {
        _pattern = fromjson(s);
        //OperationContext* opCtx = new OperationContextNoop();
        //boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(opCtx, nullptr));
        _matcher = new Matcher(_pattern, _expCtx);
        _valid = true;
    } catch (DBException& e) {
        // TODO
        //return Status(BadQuery, "Cannot parse MQL query");
        // leave _valid as false
    }
}

SearchMQL::~SearchMQL() {
    delete _matcher;
}

bool SearchMQL::matches(unsigned long doc, BSONCacheView& view) const {
    if ( ! isValid()) {
        return false;
    }
    return (_matcher->matches(view.cache()[doc], view.getMatchDetails()));
}

bool SearchMQL::isValid() const {
    return _valid;
}


class SingleLineStatus {
public:
    SingleLineStatus(BSONCache* cache = nullptr, BSONCacheView* view = nullptr)
    : _cache(cache), _view(view)
    {
    }

    SingleLineStatus(BSONCache* cache, BSONCacheView* view, TickitWindow* parent, int line = -1)
    : _cache(cache), _view(view)
    {
        init(cache, view, parent, line);
    }

    BSONCache& cache() {
        return *_cache;
    }

    BSONCacheView& view() {
        return *_view;
    }

    void init(BSONCache* cache, BSONCacheView* view, TickitWindow* parent, int line = -1) {
        _cache = cache;
        _view = view;
        _parent = parent;
        _line = line;

        _win = tickit_window_new(root, (TickitRect){ .top = ( (_line >= 0) ? _line : tickit_window_lines(_parent) - (-_line) ), .left = 0, .lines = 1, .cols = tickit_window_cols(_parent) }, (TickitWindowFlags)0);

        _pen = mkpen_highlight();

        tickit_window_bind_event(_win, TICKIT_WINDOW_ON_EXPOSE, (TickitBindFlags)0, &_render_cb, this);
    }

    void resize() {
        tickit_window_set_geometry(_win, (TickitRect){ .top = ( (_line >= 0) ? _line : tickit_window_lines(_parent) - _line ), .left = 0, .lines = 1, .cols = tickit_window_cols(_parent) });
    }

    void expose() {
        tickit_window_expose(_win, NULL);
    }

    const Date_t& getLastRenderTime() const {
        return _lastRenderTime;
    }

    void setExtra(const std::string& s) {
        _extra = s;
        expose();
    }

private:
    static int _render_cb(TickitWindow *win, TickitEventFlags flags, void *_info, void *data) {
        SingleLineStatus* status = static_cast<SingleLineStatus*>(data);
        if (status) {
            return status->_render(win, flags, _info);
        }
        return 1;
    }

    int _render(TickitWindow *win, TickitEventFlags flags, void *_info) {
        TickitExposeEventInfo *info = static_cast<TickitExposeEventInfo*>(_info);
        TickitRenderBuffer *rb = info->rb;

        tickit_renderbuffer_setpen(rb, _pen);
        tickit_renderbuffer_clear(rb);

        // TODO: elide fields that aren't needed
        tickit_renderbuffer_textf_at(rb, 0, 0,
            "%s [doc %ld] [docs %ld-%ld/%ld%s%s] [loaded %.0lf%% %.0lf/%.0lf MiB]%s%s%s",
            infname,
            view().getCursorDoc(),
            view().getStartDoc(), view().getLastDisplayedDoc(), cache().numDocs(), cache().isComplete() ? "" : "+", cache().isComplete() && view().getLastDisplayedDoc() == cache().numDocs() - 1 ? " (END)" : "",
            cache().percOfFileSeen(), cache().sizeOfFileSeen()/1048576.0, cache().sizeOfFile()/1048576.0,
            _extra == "" ? "" : " [", _extra.c_str(), _extra == "" ? "" : "]"
            );

        _lastRenderTime = Date_t::now();

        return 1;
    }


    BSONCache* _cache;
    BSONCacheView* _view;

    TickitWindow* _parent;
    TickitWindow* _win = nullptr;
    int _line;
    TickitPen* _pen;

    Date_t _lastRenderTime;

    std::string _extra;

};




BSONCache cache;
BSONCacheView view;
SingleLinePrompt prompt;
SingleLineStatus status;


int _dispatch(Tickit* t, TickitEventFlags flags, void* info, void* user) {
    std::function<void(void)>* cb = static_cast<std::function<void(void)>*>(user);
    (*cb)();
    delete cb;
    return 1;
}

void defer(std::function<void(void)> cb) {
    if (cb) {
        tickit_watch_later(t, (TickitBindFlags)0, &_dispatch, new std::function<void(void)>(cb));
    }
}


void doSearch() {

    // TODO: better user feedback (updates)
    status.setExtra("Searching...");
    defer([&] () {
        auto lastSearch = view.getLastSearch();
        if (lastSearch) {
            if ((*lastSearch)->isValid()) {
                auto doc = view.searchFor(*lastSearch);
                if (doc) {
                    status.setExtra("");
                    view.jumpToDoc(*doc);
                } else {
                    // notify the user
                    status.setExtra("Pattern not found");
                }
            } else {
                status.setExtra("Invalid search pattern");
            }
        } else {
            // notify the user
            status.setExtra("No search pattern");
        }
    });
}


void submitSearchString(const std::string& s) {
    Search *search;
    // check the format (mql etc), handle appropriately
    if (s[0] == '{') {
        search = new SearchMQL(s);
    } else {
        search = new SearchRenderedText(s);
    }

    // save the search string in history, both for n/N and up/down-arrow in search input
    view.registerSearch(search);

    doSearch();
}



static int event_key(TickitWindow *win, TickitEventFlags flags, void *_info, void *data) {
    TickitKeyEventInfo *info = static_cast<TickitKeyEventInfo*>(_info);

    if ( ! info->str) {
        return 1;
    }

    status.setExtra("");

    if (isKey(info, 'q') || isKey(info, 'Q')/* || isKey(info, "Escape")*/) {
        tickit_stop(t);

    } else if (isKey(info, '1')) {
        view.setDocumentRenderMode(BSONCacheView::kJSONOneline);

    } else if (isKey(info, '2')) {
        view.setDocumentRenderMode(BSONCacheView::kJSONPretty);

    } else if (isKey(info, '3')) {
        view.setDocumentRenderMode(BSONCacheView::kToString);

    } else if (isKey(info, '4')) {
        view.setDocumentRenderMode(BSONCacheView::kTextLogs);

    } else if (isKey(info, 's')) {
        view.toggleExtendedJSONMode();

    } else if (isKey(info, 'h') || isKey(info, "Left")) {
        view.moveLeft();

    } else if (isKey(info, 'l') || isKey(info, "Right")) {
        view.moveRight();

    } else if (isKey(info, '^') || isKey(info, '0')) {
        view.jumpLeft();

    } else if (isKey(info, '$')) {
        view.jumpRight();

    } else if (isKey(info, 'j') || isKey(info, "Down")) {
        view.moveCursorDown();

    } else if (isKey(info, 'k') || isKey(info, "Up")) {
        view.moveCursorUp();

    } else if (isKey(info, 'J') || isKey(info, "S-Down")) {
        // TODO: this should jump the cursor to the start of the next doc
        ////view.moveNextDoc();
        //view.moveCursorNextDoc();

    } else if (isKey(info, 'K') || isKey(info, "S-Up")) {
        // TODO: this should jump the cursor to the start of the prev doc
        ////view.movePrevDoc();
        //view.moveCursorPrevDoc();

    } else if (isKey(info, 'g') || isKey(info, "Home")) {
        view.jumpUp();

    } else if (isKey(info, 'G') || isKey(info, "End")) {
        view.jumpDown();

    } else if (isKey(info, 'H')) {
        view.cursorTop();

    } else if (isKey(info, 'M')) {
        view.cursorMiddle();

    } else if (isKey(info, 'L')) {
        view.cursorBottom();

    } else if (isKey(info, "PageDown") || isKey(info, "C-f") || isKey(info, ' ')) {
        view.pageDown();

    } else if (isKey(info, "PageUp") || isKey(info, "C-b")) {
        view.pageUp();

    } else if (isKey(info, '?')) {
        // TODO: show online help (key reference)

    } else if (isKey(info, "Enter")) {
        view.toggleMarkCursorDoc();

    } else if (isKey(info, "Tab")) {
        view.jumpNextMarkedDoc();

    } else if (isKey(info, "S-Tab")) {
        view.jumpPrevMarkedDoc();

    } else if (isKey(info, '/')) {
        // search forwards
        prompt.enter("/", "", submitSearchString);

    } else if (isKey(info, 'n')) {
        // search forwards again
        if (view.getLastSearch()) {
            doSearch();
        } else {
            // TODO: notify user
            status.setExtra("No previous search");
        }

    } else if (isKey(info, '{')) {
        // search forwards for doc
        prompt.enter("/", "{", submitSearchString);

    }

    return 1;
}


static int event_mouse(TickitWindow *win, TickitEventFlags flags, void *_info, void *data) {
    TickitMouseEventInfo *info = static_cast<TickitMouseEventInfo*>(_info);

    if (info->type == TICKIT_MOUSEEV_WHEEL) {
        if (info->button == TICKIT_MOUSEWHEEL_DOWN) {
            view.moveDown();
        } else {
            view.moveUp();
        }

    } else if (info->button == 1) {
        if (info->type == TICKIT_MOUSEEV_PRESS) {
            view.dragStartLine(info->line);
        } else if (info->type == TICKIT_MOUSEEV_DRAG) {
            view.dragUpdateLine(info->line);
        } else if (info->type == TICKIT_MOUSEEV_RELEASE) {
            view.dragEndLine(info->line);
        }

    }

    return 1;
}


static int render_main(TickitWindow *win, TickitEventFlags flags, void *_info, void *data) {
    TickitExposeEventInfo *info = static_cast<TickitExposeEventInfo*>(_info);
    TickitRenderBuffer *rb = info->rb;

    // wtf?  doing this makes it BOLD white on black!?
    //tickit_renderbuffer_setpen(rb, mkpen_base());

    tickit_renderbuffer_eraserect(rb, &info->rect);
    //tickit_renderbuffer_clear(rb);

    view.updateDimensions(win);
    view.drawMainLines(rb);
    view.drawTildeLines(rb);

    view.redrawStatus();

    return 1;
}


static int event_resize(TickitWindow *root, TickitEventFlags flags, void *_info, void *data) {
    int lines = tickit_window_lines(root);
    int cols = tickit_window_cols(root);

    tickit_window_set_geometry(mainwin, (TickitRect){ .top = 0, .left = 0, .lines = lines - 1, .cols = cols });
    status.resize();
    prompt.resize();

    tickit_window_expose(root, NULL);

    return 1;
}

static int load_more(Tickit *t, TickitEventFlags flags, void *_info, void *data) {
    if ( ! cache.isComplete()) {
        cache.loadSome();
        if (Date_t::now() - status.getLastRenderTime() > Milliseconds(100)) {
            view.redrawStatus();
        }
        tickit_watch_later(t, (TickitBindFlags)0, &load_more, NULL);
    } else {
        if (jumpToEndAfterLoadingComplete) {
            view.jumpDown();
        }
        view.redrawStatus();
    }
    return 0;
}



int _main(int argc, char* argv[], char** envp) {

    if (argc != 2) {
        std::cerr << "Usage: bv <bsonfile>" << std::endl;
        std::cerr << "  Exactly one input file is supported." << std::endl;
        return kInputFileError;
    }

    infname = argv[1];

    // Check that the file's fd is a regular file, no pipes or funny business.
    struct stat sb;
    if (::stat(infname, &sb) == -1) {
        int res = errno;
        std::cerr << "bv: Error: Unable to stat input file '" << infname << "': " << errnoWithDescription(res) << std::endl;
        return kInputFileError;
    }
    if ((sb.st_mode & S_IFMT) != S_IFREG) {
        std::cerr << "bv: Error: Input file '" << infname << "' is not a regular file." << std::endl;
        return kInputFileError;
    }

    // Open the file.
    const int fd = ::open(infname, O_RDONLY);
    if (fd == -1) {
        int res = errno;
        std::cerr << "bv: Error: Unable to open input file '" << infname << "': " << errnoWithDescription(res) << std::endl;
        return kInputFileError;
    }

    // Double check that the file's fd is a regular file, no pipes or funny business.
    if (::fstat(fd, &sb) == -1) {
        int res = errno;
        std::cerr << "bv: Error: Unable to fstat input file '" << infname << "': " << errnoWithDescription(res) << std::endl;
        return kInputFileError;
    }
    if ((sb.st_mode & S_IFMT) != S_IFREG) {
        std::cerr << "bv: Error: Input file '" << infname << "' is not a regular file." << std::endl;
        return kInputFileError;
    }

    // TODO: notice if the length of the file increases, and remap.  this will allow running on files that are currently downloading/uncompressing.
    // TODO: notice if the length of the file decreases, and abort.
    void* fbase = ::mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (fbase == MAP_FAILED) {
        int res = errno;
        std::cerr << "bv: Error: Unable to mmap input file '" << infname << "': " << errnoWithDescription(res) << std::endl;
        return kInputFileError;
    }

#if _POSIX_C_SOURCE >= 200112L
    if (::posix_madvise(fbase, sb.st_size, POSIX_MADV_WILLNEED) != 0) {
        int res = errno;
        std::cerr << "bv: Error: Unable to posix_madvise input file '" << infname << "': " << errnoWithDescription(res) << std::endl;
        return kInputFileError;
    }
#endif
#if _DEFAULT_SOURCE
    if (::madvise(fbase, sb.st_size, MADV_DONTDUMP) != 0) {
        int res = errno;
        std::cerr << "bv: Error: Unable to madvise input file '" << infname << "': " << errnoWithDescription(res) << std::endl;
        return kInputFileError;
    }
#endif

    const char* base = static_cast<const char*>(fbase);

    try {
        cache.init(base, base + sb.st_size);
    } catch (mongo::DBException& e) {
        std::cerr << "bv: Error: Unable to read/parse first document from input file '" << infname << "', is this a BSON file?" << std::endl;
        throw;
    }

    t = tickit_new_stdio();

    root = tickit_get_rootwin(t);
    if (!root) {
        int res = errno;
        std::cerr << "bv: Error: Unable to get root TickitWindow: " << errnoWithDescription(res) << std::endl;
        std::cerr << "bv: Check your $TERM variable, or try a different terminal emulator." << std::endl;
        return kTermError;
    }

    auto lines = tickit_window_lines(root);
    auto cols = tickit_window_cols(root);

    mainwin = tickit_window_new(root, (TickitRect){ .top = 0, .left = 0, .lines = lines - 1, .cols = cols }, (TickitWindowFlags)0);
    tickit_window_bind_event(mainwin, TICKIT_WINDOW_ON_EXPOSE, (TickitBindFlags)0, &render_main, NULL);

    tickit_window_bind_event(mainwin, TICKIT_WINDOW_ON_KEY, (TickitBindFlags)0, &event_key, NULL);
    tickit_window_bind_event(mainwin, TICKIT_WINDOW_ON_MOUSE, (TickitBindFlags)0, &event_mouse, NULL);

    view.init(&cache, [] () { tickit_window_expose(root, NULL); }, [] () { status.expose(); });

    status.init(&cache, &view, root);

    prompt.init(root, mainwin);

    tickit_window_bind_event(root, TICKIT_WINDOW_ON_GEOMCHANGE, (TickitBindFlags)0, &event_resize, NULL);

    tickit_window_take_focus(mainwin);
    tickit_window_set_cursor_visible(mainwin, false);

    tickit_watch_later(t, (TickitBindFlags)0, &load_more, NULL);

    tickit_run(t);

    return 0;
}


void tickitDone() {
    if (root) {
        tickit_window_close(root);
        root = nullptr;
    }
    if (t) {
        tickit_unref(t);
        t = nullptr;
    }
}

int main(int argc, char* argv[], char** envp) {
    int returnCode;
    try {
        returnCode = _main(argc, argv, envp);
        tickitDone();
    } catch (mongo::DBException& e) {
        tickitDone();
        std::cerr << "SEVERE: exception: " << e.what() << std::endl;
        std::cerr << "ERROR: exiting with code " << static_cast<int>(kDBException) << std::endl;
        returnCode = kDBException;
    }
    quickExit(returnCode);
}
