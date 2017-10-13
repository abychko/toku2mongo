// toku2mongo.cpp
/**
*    Copyright (C) 2014 Tokutek Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "mongo/pch.h"

#include "mongo/tools/tool.h"

#include "mongo/base/initializer.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/misc.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/client.h"
#include "mongo/db/gtid.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/oplogreader.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/password.h"
#include "mongo/db/ops/update_internal.h"
#include "mongo/platform/unordered_set.h"

#include <exception>
#include <fstream>
#include <iostream>
#include <string>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

using namespace mongo;

namespace po = boost::program_options;

class TokuOplogTool : public Tool {

    BSONObj primaryKeyToIdKey(const BSONObj &pk) {
        /* The last field of any primary key _must_ be the _id field. */
        BSONElement e;
        for (BSONObjIterator it(pk); it.more(); ++it) {
            e = *it;
        }
        BSONObj o = BSON("_id" << e);
        LOG(2) << "primaryKeyToIdKey: pk = " << pk << ", o = " << o << endl;
        return o;
    }

    bool applyOps(BSONElement opsArray) {
        verify(opsArray.type() == mongo::Array);
        for (BSONObjIterator it(opsArray.Obj()); it.more(); ++it) {
            const BSONObj op = (*it).Obj();
            const StringData type = op[OplogHelpers::KEY_STR_OP_NAME].Stringdata();
            if (type == OplogHelpers::OP_STR_COMMENT) {
                // noop
                continue;
            }
            if (op[OplogHelpers::KEY_STR_NS].type() != String) {
                error() << "invalid ns in op " << op << endl;
                return false;
            }
            string ns_str = op[OplogHelpers::KEY_STR_NS].valuestrsafe();
            if (!_only.empty()) {
                unordered_set<string>::const_iterator it = _only.find(ns_str);
                if (it == _only.end()) {
                  continue;
                }
            }
            if (!_ignore.empty()) {
                unordered_set<string>::const_iterator it = _ignore.find(ns_str);
                if (it != _ignore.end()) {
                  continue;
                }
            }
            NamespaceString ns(ns_str);
            map<string, string>::const_iterator newDbIt = _renameDatabase.find(ns.db);
            if (newDbIt != _renameDatabase.end()) {
                ns.db = newDbIt->second;
                ns_str = ns.ns();
            }
            if (_onlyProject.size() > 0) {
                const BSONElement elem = op[OplogHelpers::KEY_STR_ROW];
                if (elem.isABSONObj()) {
                    const BSONObj obj = elem.Obj();
                    if (obj["company_id"].type() == String) {
                        string company_id = obj["company_id"].valuestr();
                        if (_onlyProject != company_id) {
                            continue;
                        }
                    }
                }
            }
            if (_migrateEventsCollection && ns.coll == "events") {
                const BSONObj obj = op[OplogHelpers::KEY_STR_ROW].Obj();
                if (obj["company_id"].type() != String) {
                    error() << "invalid or missing company_id in events op" << endl;
                    return false;
                }
                string company_id = obj["company_id"].valuestr();
                std::replace(company_id.begin(), company_id.end(), '-', '_');
                ns.coll = "events_" + company_id;
                ns_str = ns.ns();
            }
            if (type == OplogHelpers::OP_STR_INSERT || type == OplogHelpers::OP_STR_CAPPED_INSERT) {
                LOG(2) << "insert: " << ns_str << endl;
                BSONObj obj = op[OplogHelpers::KEY_STR_ROW].Obj();
                if (boost::starts_with(ns.coll, "system.")) {
                    string inner_ns_str = obj["ns"].valuestrsafe();
                    if (!inner_ns_str.empty()) {
                        NamespaceString inner_ns(inner_ns_str);
                        map<string, string>::const_iterator newDbIt = _renameDatabase.find(inner_ns.db);
                        if (newDbIt != _renameDatabase.end()) {
                            inner_ns.db = newDbIt->second;
                            BSONObj obj2 = obj.removeField("ns");
                            obj = BSONObjBuilder().appendElements(obj2).append("ns", inner_ns.ns()).obj();
                        }
                    }
                    _conn->insert(ns_str, obj);
                } else {
                    const BSONElement idElem = obj["_id"];
                    if (!idElem.ok()) {
                        error() << "document to insert is missing _id field: " << obj << endl;
                        return false;
                    }
                    BSONObj id = BSON("_id" << idElem);
                    _conn->update(ns_str, id, obj, true, false);
                }
            } else if (type == OplogHelpers::OP_STR_UPDATE) {
                LOG(2) << "update: " << ns_str << endl;
                const BSONObj newObj = op[OplogHelpers::KEY_STR_NEW_ROW].Obj();
                const BSONObj id = primaryKeyToIdKey(op[OplogHelpers::KEY_STR_PK].Obj());
                _conn->update(ns_str, id, newObj, true, false);
            } else if (type == OplogHelpers::OP_STR_UPDATE_ROW_WITH_MOD) {
                LOG(2) << "update with mod: " << ns_str << endl;
                const BSONObj id = primaryKeyToIdKey(op[OplogHelpers::KEY_STR_PK].Obj());
                const BSONObj mods = op[OplogHelpers::KEY_STR_MODS].Obj();
                const BSONElement oldRow = op[OplogHelpers::KEY_STR_OLD_ROW];
                if (oldRow.ok()) {
                    // we have old document, compute the update from that
                    if (mods.isEmpty()) {
                        error() << "empty document modification" << op << endl;
                        return false;
                    }
                    IndexPathSet emptyIndexPathSet;
                    // Copied from OplogHelpers::runUpdateModsWithRowWithLock
                    // mongo/db/oplog_helpers.cpp:569 in tokumx version from github.com/7segments/mongo
                    // mongo/db/oplog_helpers.cpp:546 in tokumx version from this repo
                    BSONObj oldRowObj = oldRow.Obj(); // need to keep this in variable as it has to outlive modSet
                    scoped_ptr<ModSet> modSet(new ModSet(mods, emptyIndexPathSet));
                    auto_ptr<ModSetState> mss = modSet->prepare(oldRowObj);
                    BSONObj newObj = mss->createNewFromMods();
                    _conn->update(ns_str, id, newObj, true, false);
                } else {
                    // no old document, just replay as upsert
                    _conn->update(ns_str, id, mods, true, false);
                }
            } else if (type == OplogHelpers::OP_STR_DELETE || type == OplogHelpers::OP_STR_CAPPED_DELETE) {
                LOG(2) << "remove: " << ns_str << endl;
                const BSONObj obj = op[OplogHelpers::KEY_STR_ROW].Obj();
                const BSONElement idElem = obj["_id"];
                if (!idElem.ok()) {
                    error() << "document to insert is missing _id field: " << obj << endl;
                    return false;
                }
                BSONObj id = BSON("_id" << idElem);
                _conn->remove(ns_str, id, true);
            } else if (type == OplogHelpers::OP_STR_COMMAND) {
                const BSONObj cmd = op[OplogHelpers::KEY_STR_ROW].Obj();
                if (!ns.isCommand()) {
                    error() << "invalid command op " << op << endl;
                    return false;
                }
                LOG(2) << "cmd: " << ns_str << endl;
                BSONObj info = _conn->findOne(ns_str, cmd);
                if (!info["ok"].trueValue()) {
                    if (!cmd["create"].eoo() && info["code"].numberInt() == 48) {
                        // we are trying to create a collection, but the collection already exists
                        // so ignore the error
                        continue;
                    }
                    error() << "error replaying command op " << op << ": " << info << endl;
                    return false;
                }
            } else {
                error() << "unrecognized type \"" << type << "\" in op " << op << endl;
                return false;
            }

            std::string lastErr = _conn->getLastError(ns.db, false, false, _w);
            if (!lastErr.empty()) {
                error() << lastErr << endl;
                return false;
            }
        }
        return true;
    }

    int _run() {
        if (hasParam("renameDatabase")) {
            string param = getParam("renameDatabase");
            vector<std::string> params;
            boost::split(params, param, boost::is_any_of(":"));
            if (params.size() % 2 != 0) {
                log() << "--renameDatabase requires even number of parts" << endl;
                return -1;
            }
            for (size_t i = 0; i + 1 < params.size(); i += 2) {
                _renameDatabase[params[i]] = params[i + 1];
            }
        }

        if (hasParam("ignore")) {
            string param = getParam("ignore");
            boost::split(_ignore, param, boost::is_any_of(","));
        }

        if (hasParam("only")) {
            string param = getParam("only");
            boost::split(_only, param, boost::is_any_of(","));
        }

        if ( ! hasParam( "from" ) ) {
            log() << "need to specify --from" << endl;
            return -1;
        }

        if (currentClient.get() == 0) {
            Client::initThread( "toku2mongo" );
        }

        LOG(1) << "going to connect" << endl;
        
        OplogReader r(false);
        r.connect( getParam( "from" ) );

        if (hasParam("ruser")) {
            if (!hasParam("rpass")) {
                log() << "if using auth on source, must specify both --ruser and --rpass" << endl;
                return -1;
            }
            try {
                r.conn()->auth(BSON("user" << getParam("ruser") <<
                                    "userSource" << _rauthenticationDatabase <<
                                    "pwd" << _rpass <<
                                    "mechanism" << _authenticationMechanism));
            } catch (DBException &e) {
                if (e.getCode() == ErrorCodes::AuthenticationFailed) {
                    error() << "error authenticating to " << _rauthenticationDatabase << " on source: "
                            << e.what() << endl;
                    return -1;
                }
                throw;
            }
        }

        LOG(1) << "connected" << endl;

        {
            string gtidString;
            if (hasParam("gtid")) {
                gtidString = getParam("gtid");
            } else {
                try {
                    ifstream gtidFile;
                    gtidFile.exceptions(std::ifstream::badbit | std::ifstream::failbit);
                    gtidFile.open(_gtidFilename);
                    gtidFile >> gtidString;
                    gtidFile.close();
                } catch (std::exception &e) {
                    warning() << "Couldn't read GTID from file " << _gtidFilename << ": " << e.what() << endl;
                }
            }
            if (gtidString.empty()) {
                error() << "No starting GTID provided. "
                        << "Please find the right starting point and run again with --gtid." << endl;
                return -1;
            }
            Status s = GTID::parseConciseString(gtidString, _maxGTIDSynced);
            if (!s.isOK()) {
                error() << "error parsing GTID " << gtidString << ": " << s.reason() << endl;
                return -1;
            }
        }

        try {
            while (running) {
                r.tailCheck();
                bool needsGTIDCheck = false;
                if (!r.haveCursor()) {
                    r.tailingQueryGTE(_oplogns.c_str(), _maxGTIDSynced);
                    needsGTIDCheck = true;
                }
                if (!r.haveCursor()) {
                    error() << "oplog tailing query failed for unknown reason" << endl;
                    return -1;
                }

                if (_reportingTimer.seconds() >= _reportingPeriod) {
                    report(r);
                }

                while (running && r.more()) {
                    BSONObj o = r.nextSafe();
                    LOG(3) << o << endl;

                    if (needsGTIDCheck) {
                        GTID gtid = getGTIDFromBSON("_id", o);
                        if (GTID::cmp(gtid, _maxGTIDSynced) > 0) {
                            error() << "Wanted to start from GTID " << _maxGTIDSynced.toConciseString()
                                    << " but could only find " << gtid.toConciseString() << "." << endl;
                            error() << "You appear to have fallen too far behind in replication,"
                                    << " the destination needs to start from scratch." << endl;
                            return -1;
                        } else if (GTID::cmp(gtid, _maxGTIDSynced) < 0) {
                            error() << "Wanted to start from GTID " << _maxGTIDSynced.toConciseString()
                                    << " but we found " << gtid.toConciseString()
                                    << ", which is earlier." << endl;
                            error() << "This seems like a query bug, please report it." << endl;
                            return -1;
                        }
                        needsGTIDCheck = false;
                        // Skip the first op, we believe we've already applied it.
                        continue;
                    }
            
                    if (o.hasElement("ref")) {
                        OID oid = o["ref"].OID();
                        long long seq = 0;
                        for (shared_ptr<DBClientCursor> refsCursor = r.getOplogRefsCursor(oid); refsCursor->more(); ) {
                            BSONObj refsObj = refsCursor->nextSafe();
                            if (refsObj.getFieldDotted("_id.oid").OID() != oid) {
                                break;
                            }
                            long long thisSeq = refsObj.getFieldDotted("_id.seq").Long();
                            if (thisSeq < seq + 1) {
                                error() << "broken sequence of refs entries, last seq was " << seq << " but next object found was " << refsObj << endl;
                                return -1;
                            }
                            seq = thisSeq;
                            if (!applyOps(refsObj["ops"])) {
                                return -1;
                            }

                            if (_reportingTimer.seconds() >= _reportingPeriod) {
                                report(r);
                            }
                        }
                    } else if (o.hasElement("ops")) {
                        if (!applyOps(o["ops"])) {
                            return -1;
                        }
                    } else {
                        error() << "invalid oplog entry " << o << endl;
                        return -1;
                    }

                    _maxGTIDSynced = getGTIDFromBSON("_id", o);
                    _maxTimestampSynced = o["ts"].Date();

                    if (_reportingTimer.seconds() >= _reportingPeriod) {
                        report(r);
                    }
                }
            }
        } catch (DBException &e) {
            error() << "Caught exception " << e.what() << ". Exiting..." << endl;
            return -1;
        } catch (...) {
            error() << "Caught unknown exception, Exiting..." << endl;
            return -1;
        }

        return 0;
    }

    void report(OplogReader &rdr) const {
        Nullstream& l = log();
        l << "synced up to " << _maxGTIDSynced.toConciseString()
          << " (" << time_t_to_String_short(_maxTimestampSynced.toTimeT()) << ")";
        _reportingTimer.reset();
        BSONObj lastObj = rdr.getLastOp(_oplogns.c_str());
        GTID lastGTID = getGTIDFromBSON("_id", lastObj);
        Date_t lastDate = lastObj["ts"].Date();
        l << ", source has up to " << lastGTID.toConciseString()
          << " (" << time_t_to_String_short(lastDate.toTimeT()) << ")";
        if (GTID::cmp(lastGTID, _maxGTIDSynced) == 0) {
            l << ", fully synced." << endl;
        }
        else {
            int64_t diff = lastDate.millis - _maxTimestampSynced.millis;
            if (diff > 1000) {
                l << ", " << (diff / 1000) << " seconds behind source." << endl;
            }
            else {
                l << ", less than 1 second behind source." << endl;
            }
        }
    }

    string _oplogns;
    string _rpass;
    string _rauthenticationDatabase;
    string _rauthenticationMechanism;
    string _onlyProject;
    bool _migrateEventsCollection;
    map<string, string> _renameDatabase;
    unordered_set<string> _ignore;
    unordered_set<string> _only;
    GTID _maxGTIDSynced;
    Date_t _maxTimestampSynced;
    int _w;
    int _reportingPeriod;
    mutable Timer _reportingTimer;
    static const char *_gtidFilename;

public:
    TokuOplogTool() : Tool( "2mongo" ) {
        add_options()
        ("gtid" , po::value<string>() , "max applied GTID" )
        ("w", po::value(&_w)->default_value(1), "w parameter for getLastError calls")
        ("from", po::value<string>() , "host to pull from" )
        ("renameDatabase", po::value<string>() , "rename database" )
        ("migrateEventsCollection", po::value<bool>(&_migrateEventsCollection) , "migrate events" )
        ("onlyProject", po::value<string>(&_onlyProject) , "only process docs for a single project (be careful about using this, it only handles a few specific collections)" )
        ("ignore", po::value<string>() , "comma separated list of ns to ignore" )
        ("only", po::value<string>() , "comma separated list of ns to process, ignore the rest" )
        ("ruser", po::value<string>(), "username on source host if auth required" )
        ("rpass", new PasswordValue( &_rpass ), "password on source host" )
        ("rauthenticationDatabase",
         po::value<string>(&_rauthenticationDatabase)->default_value("admin"),
         "user source on source host (defaults to \"admin\")" )
        ("rauthenticationMechanism",
         po::value<string>(&_rauthenticationMechanism)->default_value("MONGODB-CR"),
         "authentication mechanism on source host")
        ("oplogns", po::value<string>(&_oplogns)->default_value( "local.oplog.rs" ) , "ns to pull from" )
        ("reportingPeriod", po::value<int>(&_reportingPeriod)->default_value(10) , "seconds between progress reports" )
        ;
    }

    virtual void printExtraHelp(ostream& out) {
        out << "Pull and replay a remote TokuMX oplog.\n" << endl;
    }

    static volatile bool running;

    void logPosition() {
        if (GTID::cmp(_maxGTIDSynced, GTID()) != 0) {
            std::string gtidString = _maxGTIDSynced.toConciseString();
            log() << "Exiting while processing GTID " << gtidString << endl;
            log() << "Use --gtid=" << gtidString << " to resume." << endl;
            try {
                std::ofstream gtidFile;
                gtidFile.exceptions(std::ofstream::badbit | std::ofstream::failbit);
                gtidFile.open(_gtidFilename, std::ofstream::trunc);
                gtidFile << gtidString;
                gtidFile.close();
                log() << "Saved GTID to file "
                      << (boost::filesystem::current_path() / _gtidFilename).string() << "." << endl;
                log() << "I'll automatically use this value next time if you run from this directory "
                      << "and don't pass --gtid." << endl;
            } catch (std::exception &e) {
                warning() << "Error saving GTID to file " << _gtidFilename << ": " << e.what() << endl;
                warning() << "Make sure you save the GTID somewhere, because I couldn't!" << endl;
            }
        }
    }

    int run() {
        int ret = _run();
        logPosition();
        return ret;
    }
};

const char *TokuOplogTool::_gtidFilename = "__toku2mongo_saved_timestamp__";

volatile bool TokuOplogTool::running = false;

namespace proc_mgmt {

    TokuOplogTool *theTool = NULL;

    static void fatal_handler(int sig) {
        signal(sig, SIG_DFL);
        log() << "Received signal " << sig << "." << endl;
        warning() << "Dying immediately on fatal signal." << endl;
        if (theTool != NULL) {
            theTool->logPosition();
        }
        ::abort();
    }
    static void exit_handler(int sig) {
        signal(sig, SIG_DFL);
        log() << "Received signal " << sig << "." << endl;
        log() << "Will exit soon." << endl;
        TokuOplogTool::running = false;
    }

}

int main( int argc , char** argv, char **envp ) {
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    TokuOplogTool t;
    t.running = true;
    proc_mgmt::theTool = &t;
    signal(SIGILL, proc_mgmt::fatal_handler);
    signal(SIGABRT, proc_mgmt::fatal_handler);
    signal(SIGFPE, proc_mgmt::fatal_handler);
    signal(SIGSEGV, proc_mgmt::fatal_handler);
    signal(SIGHUP, proc_mgmt::exit_handler);
    signal(SIGINT, proc_mgmt::exit_handler);
    signal(SIGQUIT, proc_mgmt::exit_handler);
    signal(SIGPIPE, proc_mgmt::exit_handler);
    signal(SIGALRM, proc_mgmt::exit_handler);
    signal(SIGTERM, proc_mgmt::exit_handler);
    signal(SIGUSR1, SIG_IGN);
    signal(SIGUSR2, SIG_IGN);
    return t.main( argc , argv );
}
