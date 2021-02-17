#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "client_connection.hpp"
#include "connection_pool.hpp"
#include "irods_at_scope_exit.hpp"
#include "irods_client_api_table.hpp"
#include "irods_pack_table.hpp"
#include "dstream.hpp"
#include "transport/default_transport.hpp"

#include "filesystem.hpp"
#include "user_administration.hpp"
#include "rodsPath.h"
#include "rodsErrorTable.h"
#include "miscUtil.h"
#include "cpUtil.h"
#include "rcGlobalExtern.h"
#include "irods_virtual_path.hpp"
#include "rodsLog.h"

#include "json.hpp"

#include <fmt/format.h>

#include <cstdlib>
#include <cerrno>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <stdexcept>

namespace adm = irods::experimental::administration;
namespace ifs = irods::experimental::filesystem;
namespace sfs = std::filesystem;

static void populate(rodsPath_t *rodsPath, ifs::path path);
static char rand_char();

static irods::api_entry_table &api_table = irods::get_client_api_table();
static irods::pack_entry_table &pck_table = irods::get_pack_table();

static const std::string envkw_env_file = irods::to_env( irods::CFG_IRODS_ENVIRONMENT_FILE_KW );
static const std::string envkw_auth_file = irods::to_env( irods::CFG_IRODS_AUTHENTICATION_FILE_KW );
static char envval_env_file[MAX_NAME_LEN];
static char envval_auth_file[MAX_NAME_LEN];

static rodsEnv base_env;
static std::shared_ptr<irods::connection_pool> base_conn_pool;
static RcComm base_conn;

static std::string user1_name("user_XXXXXX");
static const std::string user1_pass("rods");
static sfs::path user1_env_file;
static sfs::path user1_auth_file;
static rodsEnv user1_env;
static std::shared_ptr<irods::connection_pool> user1_conn_pool;
static RcComm user1_conn;

static sfs::path fixture_local_sandbox;
static ifs::path fixture_rods_sandbox;

class IntValue : public Catch::MatcherBase<int> {
    int m_value;
public:
    IntValue(int value) : m_value(value) {}
    virtual bool match(int const& i) const override {
        return i == m_value;
    }
    virtual std::string describe() const override {
        std::ostringstream ss;
        ss << "is equal to " << rodsErrorName(m_value, NULL) << " (" << m_value << ")";
        return ss.str();
    }
};

inline IntValue IntEquals(int value) {
    return IntValue(value);
}

#if defined(CATCH_CONFIG_WCHAR) && defined(WIN32) && defined(_UNICODE) && !defined(DO_NOT_USE_WMAIN)
extern "C" int wmain (int argc, wchar_t * argv[], wchar_t * [])
#else
int main (int argc, char * argv[])
#endif
{
    int status;

    init_api_table(api_table, pck_table);

    memset( &envval_env_file, 0, sizeof(envval_env_file) );
    memset( &envval_auth_file, 0, sizeof(envval_auth_file) );
    {
        char *envpath = getenv( envkw_env_file.c_str() );
        if ( envpath != NULL ) {
            rstrcpy( envval_env_file, envpath, MAX_NAME_LEN );
        }
        char *authpath = getenv( envkw_auth_file.c_str() );
        if ( authpath != NULL ) {
            rstrcpy( envval_auth_file, authpath, MAX_NAME_LEN );
        }
    }

    status = getRodsEnv(&base_env);
    if (status < 0) {
        std::ostringstream ss;
        ss << "getRodsEnv failed: " << rodsErrorName(status, NULL) << " (" << status << ")";
        throw std::runtime_error(ss.str());
    }

    const int connection_count = 1;

    base_conn_pool = std::make_shared<irods::connection_pool>(
                connection_count,
                base_env.rodsHost,
                base_env.rodsPort,
                base_env.rodsUserName,
                base_env.rodsZone,
                base_env.irodsConnectionPoolRefreshTime);

    base_conn = base_conn_pool->get_connection();

    char fixture_local_sandbox_path[MAX_NAME_LEN];
    rstrcpy( fixture_local_sandbox_path,
             (sfs::temp_directory_path() / "fixture_sandbox-XXXXXX").c_str(),
             MAX_NAME_LEN );
    if (mkdtemp(fixture_local_sandbox_path) == NULL) {
        status = errno;
        std::ostringstream ss;
        ss << "mkdtemp failed: " << std::strerror(status) << " (" << status << ")";
        throw std::runtime_error(ss.str());
    }

    fixture_local_sandbox = sfs::path(fixture_local_sandbox_path);

    irods::at_scope_exit cleanup_fixture_local_sandbox{[] {
        if (sfs::exists(fixture_local_sandbox)) {
            sfs::remove_all(fixture_local_sandbox);
        }
    }};

    {
        size_t name_len = user1_name.length();
        for (int i = 1; i < 7; ++i) {
            user1_name[name_len - i] = rand_char();
        }
    }

    adm::user user1(user1_name);
    irods::at_scope_exit cleanup_user1{[&user1] {
        if (adm::client::exists(base_conn, user1)) {
            adm::client::remove_user(base_conn, user1);
        }
    }};

    adm::client::add_user(base_conn, user1);
    adm::client::set_user_password(base_conn, user1, user1_pass);
    adm::client::set_user_type(base_conn, user1, adm::user_type::rodsuser);

    user1_env_file = fixture_local_sandbox / fmt::format("{}_irods_environment.json", user1_name);
    user1_auth_file = fixture_local_sandbox / fmt::format("{}_irods_auth", user1_name);

    {
        nlohmann::json user1_env_json = { { "irods_host", base_env.rodsHost },
                                          { "irods_port", base_env.rodsPort },
                                          { "irods_user_name", user1_name },
                                          { "irods_zone_name", base_env.rodsZone } };
        std::ofstream user1_env_ofs(user1_env_file);
        user1_env_ofs << std::setw(4) << user1_env_json << std::endl;
        user1_env_ofs.flush();
        user1_env_ofs.close();
    }

    setenv( envkw_env_file.c_str(), user1_env_file.c_str(), true );
    setenv( envkw_auth_file.c_str(), user1_auth_file.c_str(), true );

    status = getRodsEnv(&user1_env);
    if (status < 0) {
        std::ostringstream ss;
        ss << "getRodsEnv failed: " << rodsErrorName(status, NULL) << " (" << status << ")";
        throw std::runtime_error(ss.str());
    }

    obfSavePw( 0, 0, 0, user1_pass.c_str() );

    user1_conn_pool = std::make_shared<irods::connection_pool>(
                connection_count,
                user1_env.rodsHost,
                user1_env.rodsPort,
                user1_env.rodsUserName,
                user1_env.rodsZone,
                user1_env.irodsConnectionPoolRefreshTime);

    user1_conn = user1_conn_pool->get_connection();

    if ( std::strlen(envval_env_file) == 0 ) {
        unsetenv(envkw_env_file.c_str());
    } else {
        setenv( envkw_env_file.c_str(), envval_env_file, true );
    }
    if ( std::strlen(envval_auth_file) == 0 ) {
        unsetenv(envkw_auth_file.c_str());
    } else {
        setenv( envkw_auth_file.c_str(), envval_auth_file, true );
    }

    {
        std::string sandbox_coll("fixture_sandbox-XXXXXX");
        size_t sandbox_col_len = sandbox_coll.length();
        do {
            for(int i = 1; i < 7; ++i) {
                sandbox_coll[sandbox_col_len - i] = rand_char();
            }
            fixture_rods_sandbox = ifs::path{"/"} / base_env.rodsZone / sandbox_coll;
        } while (ifs::client::exists(base_conn, fixture_rods_sandbox));
    }
    ifs::client::create_collection(base_conn, fixture_rods_sandbox);

    irods::at_scope_exit cleanup_fixture_rods_sandbox{[] {
        if (ifs::client::exists(base_conn, fixture_rods_sandbox)) {
            ifs::client::remove_all(base_conn, fixture_rods_sandbox);
        }
    }};

    return Catch::Session().run( argc, argv );

}

/*
 * srcPath - sources
 * destPath - destination, not an array
 * targPath - final new paths
 */

TEST_CASE("resolveRodsTarget")
{
    using odstream          = irods::experimental::io::odstream;
    using default_transport = irods::experimental::io::client::default_transport;

    const int initRodsLogLevel = getRodsLogLevel();

    ifs::path sandbox;
    {
        std::string sandbox_subcoll("unit_testing_sandbox-XXXXXX");
        size_t sandbox_subcol_len = sandbox_subcoll.length();
        do {
            for(int i = 1; i < 7; ++i) {
                sandbox_subcoll[sandbox_subcol_len - i] = rand_char();
            }

            sandbox = ifs::path{"/"} / base_env.rodsZone / sandbox_subcoll;
        } while (ifs::client::exists(base_conn, sandbox));
    }

    REQUIRE(ifs::client::create_collection(base_conn, sandbox));
    REQUIRE(ifs::client::exists(base_conn, sandbox));

    irods::at_scope_exit cleanup{[&sandbox, &initRodsLogLevel] {
        // restore log level
        rodsLogLevel(initRodsLogLevel);
        REQUIRE(ifs::client::remove_all(base_conn, sandbox, ifs::remove_options::no_trash));
    }};

    {
    default_transport tp{base_conn};
        for(int i = 0; i < 5; ++i) {
            ifs::path test_data = sandbox / fmt::format("test_data{}", i);
            odstream{tp, test_data};
            REQUIRE(ifs::client::exists(base_conn, test_data));

            ifs::path test_coll1 = sandbox / fmt::format("test_coll{}", i);
            REQUIRE(ifs::client::create_collection(base_conn, test_coll1));
            REQUIRE(ifs::client::exists(base_conn, test_coll1));

            ifs::path test_coll2 = sandbox / fmt::format("test_coll{}", i + 5);
            REQUIRE(ifs::client::create_collection(base_conn, test_coll2));
            REQUIRE(ifs::client::exists(base_conn, test_coll2));

            for(int j = i; j < 5; ++j) {
                ifs::path test_data2 = test_coll1 / fmt::format("test_data{}_{}", i, j);
                odstream{tp, test_data2};
                REQUIRE(ifs::client::exists(base_conn, test_data2));

                ifs::path test_data3 = test_coll2 / fmt::format("test_data{}", j);
                odstream{tp, test_data3};
                REQUIRE(ifs::client::exists(base_conn, test_data3));
            }
        }
    }

    // no logging after initialization
    rodsLogLevel(LOG_SYS_WARNING);

    rodsPath_t rodsPath_dest{};

    SECTION("single_source")
    {
        rodsPath_t rodsPath_src{};
        rodsPath_t rodsPath_targ{};
        rodsPathInp_t rodsPathInp{
            .numSrc = 1,
            .srcPath = &rodsPath_src,
            .destPath = &rodsPath_dest,
            .targPath = &rodsPath_targ,
        };

        SECTION("source_collection")
        {
            ifs::path src_coll = sandbox / "test_coll2";
            ifs::path src_coll_cont1 = src_coll / "test_data2_3";
            REQUIRE(ifs::client::exists(base_conn, src_coll));
            REQUIRE(ifs::client::exists(base_conn, src_coll_cont1));

            populate(&rodsPath_src, src_coll);

            SECTION("destination_parent_exists")
            {
                ifs::path dest_coll_parent = sandbox / "test_coll3";
                ifs::path dest_coll = dest_coll_parent / "test_coll2";
                ifs::path dest_coll_cont1 = dest_coll / "test_data2_3";

                REQUIRE(ifs::client::exists(base_conn, dest_coll_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_cont1));

                SECTION("destination_parent")
                {
                    populate(&rodsPath_dest, dest_coll_parent);

                    SECTION("PUT_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                        REQUIRE(dest_coll.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                        REQUIRE(ifs::client::exists(base_conn, dest_coll));
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_cont1));
                    }

                    SECTION("COPY_DEST")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST));

                        REQUIRE(dest_coll.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));
                    }

                    SECTION("MOVE_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR));

                        REQUIRE(dest_coll.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));
                    }

                    SECTION("RSYNC_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                        REQUIRE(dest_coll_parent.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));
                    }
                }

                SECTION("destination_full")
                {
                    populate(&rodsPath_dest, dest_coll);

                    SECTION("PUT_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                        REQUIRE(dest_coll.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                        REQUIRE(ifs::client::exists(base_conn, dest_coll));
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_cont1));
                    }

                    SECTION("COPY_DEST")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST));

                        REQUIRE(dest_coll.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));
                    }

                    SECTION("MOVE_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR));

                        REQUIRE(dest_coll.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));
                    }

                    SECTION("RSYNC_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                        REQUIRE(dest_coll.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                        REQUIRE(ifs::client::exists(base_conn, dest_coll));
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_cont1));
                    }
                }
            }

            SECTION("destination_parent_missing")
            {
                ifs::path dest_coll_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_coll = dest_coll_parent / "test_coll2";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));

                populate(&rodsPath_dest, dest_coll);

                SECTION("PUT_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }
            }

            SECTION("destination_parent_parent_missing")
            {
                ifs::path dest_coll_parent_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_coll_parent = dest_coll_parent_parent / "test_coll_nonexist2";
                ifs::path dest_coll = dest_coll_parent / "test_coll2";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));

                populate(&rodsPath_dest, dest_coll);

                SECTION("PUT_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }
            }
        }

        SECTION("source_dataobj")
        {
            ifs::path src_data = sandbox / "test_data4";
            REQUIRE(ifs::client::exists(base_conn, src_data));

            populate(&rodsPath_src, src_data);

            SECTION("destination_parent_exists")
            {
                ifs::path dest_data_parent = sandbox / "test_coll3";
                ifs::path dest_data = dest_data_parent / "test_data4";

                REQUIRE(ifs::client::exists(base_conn, dest_data_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));

                SECTION("destination_parent")
                {
                    populate(&rodsPath_dest, dest_data_parent);

                    SECTION("PUT_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                        REQUIRE(dest_data.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                    }

                    SECTION("COPY_DEST")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST));

                        REQUIRE(dest_data.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                    }

                    SECTION("MOVE_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR));

                        REQUIRE(dest_data.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                    }

                    SECTION("RSYNC_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                        REQUIRE(dest_data.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                        REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                    }
                }

                SECTION("destination_full")
                {
                    populate(&rodsPath_dest, dest_data);

                    SECTION("PUT_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                        REQUIRE(dest_data.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                        CHECK(rodsPathInp.targPath[0].objState == NOT_EXIST_ST);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                    }

                    SECTION("COPY_DEST")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST));

                        REQUIRE(dest_data.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                        CHECK(rodsPathInp.targPath[0].objState == NOT_EXIST_ST);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                    }

                    SECTION("MOVE_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR));

                        REQUIRE(dest_data.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                        CHECK(rodsPathInp.targPath[0].objState == NOT_EXIST_ST);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                    }

                    SECTION("RSYNC_OPR")
                    {
                        REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                        REQUIRE(dest_data.string() == rodsPathInp.targPath->outPath);
                        REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                        REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                        REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                        REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                        CHECK(rodsPathInp.targPath[0].objState == NOT_EXIST_ST);
                        REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                    }
                }
            }

            SECTION("destination_parent_missing")
            {
                ifs::path dest_data_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_data = dest_data_parent / "test_data4";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));

                populate(&rodsPath_dest, dest_data);

                SECTION("PUT_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }
            }

            SECTION("destination_parent_parent_missing")
            {
                ifs::path dest_data_parent_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_data_parent = dest_data_parent_parent / "test_coll_nonexist2";
                ifs::path dest_data = dest_data_parent / "test_data4";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));

                populate(&rodsPath_dest, dest_data);

                SECTION("PUT_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }
            }
        }
    }

    SECTION("multi_source_2")
    {
        rodsPath_t rodsPath_src[2] = { rodsPath_t{}, rodsPath_t{} };
        rodsPath_t rodsPath_targ[2] = { rodsPath_t{}, rodsPath_t{} };
        rodsPathInp_t rodsPathInp{
            .numSrc = 2,
            .srcPath = rodsPath_src,
            .destPath = &rodsPath_dest,
            .targPath = rodsPath_targ,
        };

        SECTION("source_collection")
        {
            ifs::path src_coll1 = sandbox / "test_coll1";
            ifs::path src_coll2 = sandbox / "test_coll2";
            ifs::path src_coll1_cont1 = src_coll1 / "test_data1_1";
            ifs::path src_coll2_cont1 = src_coll2 / "test_data2_3";
            REQUIRE(ifs::client::exists(base_conn, src_coll1));
            REQUIRE(ifs::client::exists(base_conn, src_coll2));
            REQUIRE(ifs::client::exists(base_conn, src_coll1_cont1));
            REQUIRE(ifs::client::exists(base_conn, src_coll2_cont1));

            populate(&rodsPath_src[0], src_coll1);
            populate(&rodsPath_src[1], src_coll2);

            SECTION("destination_parent_exists")
            {
                ifs::path dest_coll_parent = sandbox / "test_coll3";
                ifs::path dest_coll1 = dest_coll_parent / "test_coll1";
                ifs::path dest_coll2 = dest_coll_parent / "test_coll2";
                ifs::path dest_coll1_cont1 = dest_coll1 / "test_data1_1";
                ifs::path dest_coll2_cont1 = dest_coll2 / "test_data2_3";

                REQUIRE(ifs::client::exists(base_conn, dest_coll_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll1));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll2));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll1_cont1));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll2_cont1));

                populate(&rodsPath_dest, dest_coll_parent);

                SECTION("PUT_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                    REQUIRE(dest_coll1.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_coll2.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == COLL_OBJ_T);
                    REQUIRE(ifs::client::exists(base_conn, dest_coll1));
                    REQUIRE(ifs::client::exists(base_conn, dest_coll2));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll1_cont1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll2_cont1));
                }

                SECTION("COPY_DEST")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST));

                    REQUIRE(dest_coll1.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_coll2.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == COLL_OBJ_T);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll2));
                }

                SECTION("MOVE_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR));

                    REQUIRE(dest_coll1.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_coll2.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == COLL_OBJ_T);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll2));
                }

                SECTION("RSYNC_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                    REQUIRE(dest_coll1.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_coll2.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == COLL_OBJ_T);
                    REQUIRE(ifs::client::exists(base_conn, dest_coll1));
                    REQUIRE(ifs::client::exists(base_conn, dest_coll2));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll1_cont1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll2_cont1));
                }
            }

            SECTION("destination_parent_missing")
            {
                ifs::path dest_coll_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_coll1 = dest_coll_parent / "test_coll1";
                ifs::path dest_coll2 = dest_coll_parent / "test_coll2";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll1));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll2));

                populate(&rodsPath_dest, dest_coll_parent);

                SECTION("PUT_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(ifs::client::exists(base_conn, dest_coll_parent));
                    REQUIRE(ifs::client::exists(base_conn, dest_coll1));
                    REQUIRE(ifs::client::exists(base_conn, dest_coll2));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(ifs::client::exists(base_conn, dest_coll_parent));
                    REQUIRE(ifs::client::exists(base_conn, dest_coll1));
                    REQUIRE(ifs::client::exists(base_conn, dest_coll2));
                }
            }

            SECTION("destination_parent_parent_missing")
            {
                ifs::path dest_coll_parent_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_coll_parent = dest_coll_parent_parent / "test_coll_nonexist2";
                ifs::path dest_coll1 = dest_coll_parent / "test_coll1";
                ifs::path dest_coll2 = dest_coll_parent / "test_coll2";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll1));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll2));

                populate(&rodsPath_dest, dest_coll_parent);

                SECTION("PUT_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_coll_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_parent));
                }
            }
        }

        SECTION("source_dataobj")
        {
            ifs::path src_data1 = sandbox / "test_data3";
            ifs::path src_data2 = sandbox / "test_data4";
            REQUIRE(ifs::client::exists(base_conn, src_data1));
            REQUIRE(ifs::client::exists(base_conn, src_data2));

            populate(&rodsPath_src[0], src_data1);
            populate(&rodsPath_src[1], src_data2);

            SECTION("destination_parent_exists")
            {
                ifs::path dest_data_parent = sandbox / "test_coll3";
                ifs::path dest_data1 = dest_data_parent / "test_data3";
                ifs::path dest_data2 = dest_data_parent / "test_data4";

                REQUIRE(ifs::client::exists(base_conn, dest_data_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data1));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data2));

                populate(&rodsPath_dest, dest_data_parent);

                SECTION("PUT_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                    REQUIRE(dest_data1.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_data2.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == DATA_OBJ_T);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data2));
                }

                SECTION("COPY_DEST")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST));

                    REQUIRE(dest_data1.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_data2.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == DATA_OBJ_T);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data2));
                }

                SECTION("MOVE_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR));

                    REQUIRE(dest_data1.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_data2.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == DATA_OBJ_T);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data2));
                }

                SECTION("RSYNC_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                    REQUIRE(dest_data1.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_data2.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == DATA_OBJ_T);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data2));
                }
            }

            SECTION("destination_parent_missing")
            {
                ifs::path dest_data_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_data1 = dest_data_parent / "test_data3";
                ifs::path dest_data2 = dest_data_parent / "test_data4";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data1));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data2));

                populate(&rodsPath_dest, dest_data_parent);

                SECTION("PUT_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(ifs::client::exists(base_conn, dest_data_parent));
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_data1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data2));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(ifs::client::exists(base_conn, dest_data_parent));
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_data1));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data2));
                }
            }

            SECTION("destination_parent_parent_missing")
            {
                ifs::path dest_data_parent_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_data_parent = dest_data_parent_parent / "test_coll_nonexist2";
                ifs::path dest_data1 = dest_data_parent / "test_data3";
                ifs::path dest_data2 = dest_data_parent / "test_data4";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data1));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data2));

                populate(&rodsPath_dest, dest_data_parent);

                SECTION("PUT_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_data_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data_parent));
                }
            }
        }

        SECTION("source_mixed")
        {
            ifs::path src_coll = sandbox / "test_coll2";
            ifs::path src_data = sandbox / "test_data4";
            ifs::path src_coll_cont1 = src_coll / "test_data2_3";
            REQUIRE(ifs::client::exists(base_conn, src_coll));
            REQUIRE(ifs::client::exists(base_conn, src_data));
            REQUIRE(ifs::client::exists(base_conn, src_coll_cont1));

            populate(&rodsPath_src[0], src_coll);
            populate(&rodsPath_src[1], src_data);

            SECTION("destination_parent_exists")
            {
                ifs::path dest_both_parent = sandbox / "test_coll3";
                ifs::path dest_coll = dest_both_parent / "test_coll2";
                ifs::path dest_data = dest_both_parent / "test_data4";
                ifs::path dest_coll_cont1 = dest_coll / "test_data2_3";

                REQUIRE(ifs::client::exists(base_conn, dest_both_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_cont1));

                populate(&rodsPath_dest, dest_both_parent);

                SECTION("PUT_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                    REQUIRE(dest_coll.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_data.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == DATA_OBJ_T);
                    CHECK(ifs::client::exists(base_conn, dest_coll));
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_data));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_cont1));
                }

                SECTION("COPY_DEST")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST));

                    REQUIRE(dest_coll.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_data.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == DATA_OBJ_T);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_coll));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                }

                SECTION("MOVE_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR));

                    REQUIRE(dest_coll.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_data.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == DATA_OBJ_T);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_coll));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                }

                SECTION("RSYNC_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                    REQUIRE(dest_coll.string() == rodsPathInp.targPath[0].outPath);
                    REQUIRE(dest_data.string() == rodsPathInp.targPath[1].outPath);
                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(rodsPathInp.targPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.targPath[1].objType == DATA_OBJ_T);
                    CHECK(ifs::client::exists(base_conn, dest_coll));
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_data));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll_cont1));
                }
            }

            SECTION("destination_parent_missing")
            {
                ifs::path dest_both_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_coll = dest_both_parent / "test_coll2";
                ifs::path dest_data = dest_both_parent / "test_data4";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_both_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));

                populate(&rodsPath_dest, dest_both_parent);

                SECTION("PUT_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(ifs::client::exists(base_conn, dest_both_parent));
                    CHECK(ifs::client::exists(base_conn, dest_coll));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_both_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_both_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    REQUIRE_FALSE(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == EXIST_ST);
                    REQUIRE(ifs::client::exists(base_conn, dest_both_parent));
                    CHECK(ifs::client::exists(base_conn, dest_coll));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));
                }
            }

            SECTION("destination_parent_parent_missing")
            {
                ifs::path dest_both_parent_parent = sandbox / "test_coll_nonexist";
                ifs::path dest_both_parent = dest_both_parent_parent / "test_coll_nonexist2";
                ifs::path dest_coll = dest_both_parent / "test_coll2";
                ifs::path dest_data = dest_both_parent / "test_data4";

                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_both_parent_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_both_parent));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_coll));
                REQUIRE_FALSE(ifs::client::exists(base_conn, dest_data));

                populate(&rodsPath_dest, dest_both_parent);

                SECTION("PUT_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, PUT_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_both_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_both_parent));
                }

                SECTION("COPY_DEST")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, COPY_DEST),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_both_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_both_parent));
                }

                SECTION("MOVE_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, MOVE_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_both_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_both_parent));
                }

                SECTION("RSYNC_OPR")
                {
                    CHECK_THAT(resolveRodsTarget(static_cast<rcComm_t*>(&base_conn), &rodsPathInp, RSYNC_OPR),
                                 IntEquals(CAT_UNKNOWN_COLLECTION) || IntEquals(USER_FILE_DOES_NOT_EXIST));

                    REQUIRE(rodsPathInp.srcPath[0].objType == COLL_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[0].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.srcPath[1].objType == DATA_OBJ_T);
                    REQUIRE(rodsPathInp.srcPath[1].objState == EXIST_ST);
                    REQUIRE(rodsPathInp.destPath->objType == UNKNOWN_OBJ_T);
                    REQUIRE(rodsPathInp.destPath->objState == NOT_EXIST_ST);
                    CHECK_FALSE(ifs::client::exists(base_conn, dest_both_parent_parent));
                    REQUIRE_FALSE(ifs::client::exists(base_conn, dest_both_parent));
                }
            }
        }
    }
}

inline static void populate(rodsPath_t *rodsPath, ifs::path path) {
    const char *szPath = path.c_str();
    rstrcpy(rodsPath->inPath, szPath, MAX_NAME_LEN);
    rstrcpy(rodsPath->outPath, szPath, MAX_NAME_LEN);
}

static std::string chars("abcdefghijklmnopqrstuvwxyz1234567890");

#if CATCH_VERSION_MAJOR > 2 || (CATCH_VERSION_MAJOR == 2 && CATCH_VERSION_MINOR >= 10)
static Catch::Generators::RandomIntegerGenerator<int> index_dist(0, chars.size() - 1);
inline static char rand_char() {
    index_dist.next();
    return chars[index_dist.get()];
}
#else
#include <random>
static std::uniform_int_distribution<> index_dist(0, chars.size() - 1);
inline static char rand_char() {
    return chars[index_dist(Catch::rng())];
}
#endif
