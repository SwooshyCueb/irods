//
// The following unit tests were implemented based on code examples from
// "cppreference.com". This code is licensed under the following:
//
//   - Creative Commons Attribution-Sharealike 3.0 Unported License (CC-BY-SA)
//   - GNU Free Documentation License (GFDL)
//
// For more information about these licenses, visit:
//
//   - https://en.cppreference.com/w/Cppreference:FAQ
//   - https://en.cppreference.com/w/Cppreference:Copyright/CC-BY-SA
//   - https://en.cppreference.com/w/Cppreference:Copyright/GDFL
//

#include <catch2/catch.hpp>

#include <string>
#include <sstream>
#include <vector>
#include <tuple>
#include <optional>

#include <irods/irods_plugin_base.hpp>
#include <fmt/format.h>
#include "irods/filesystem/path.hpp"

std::tuple<bool, std::optional<std::string>> get_detached_mode_vault_path(
        irods::plugin_property_map& prop_map,
        const std::string& resource_hostname,
        const std::string& resource_name);

TEST_CASE("detached mode vault path", "[detached_mode_vault_path]")
{

    std::string resource_hostname = "myhost";
    std::string resource_name = "my_resc";

    SECTION("not in host list")
    {
        irods::plugin_property_map prop_map;
        prop_map.set<std::string>("HOST_LIST", "host2:/host2/path,host3");

        bool in_host_list;
        std::optional<std::string> new_vault_path_optional;

        std::tie(in_host_list, new_vault_path_optional) =
            get_detached_mode_vault_path(prop_map, resource_hostname, resource_name);

        REQUIRE(in_host_list == false);
        REQUIRE(new_vault_path_optional.has_value() == false);
    }

    SECTION("not in host list but hostname with common prefix")
    {
        irods::plugin_property_map prop_map;
        prop_map.set<std::string>("HOST_LIST", "myhost1,myhost2,host2:/host2/path,host3,myhost3");

        bool in_host_list;
        std::optional<std::string> new_vault_path_optional;

        std::tie(in_host_list, new_vault_path_optional) =
            get_detached_mode_vault_path(prop_map, resource_hostname, resource_name);

        REQUIRE(in_host_list == false);
        REQUIRE(new_vault_path_optional.has_value() == false);
    }

    /* The following test causes the unixfilesystem plugin to log a warning
     * using the new logger.
     * This causes a SEGV because the unit test does not have the
     * environment to do logging.
     * Commenting this out for now.
    SECTION("in host list but relative vault path")
    {
        irods::plugin_property_map prop_map;
        prop_map.set<std::string>("HOST_LIST", "myhost:relative/path,host2:/host2/path,host3");

        bool in_host_list;
        std::optional<std::string> new_vault_path_optional;

        std::tie(in_host_list, new_vault_path_optional) =
            get_detached_mode_vault_path(prop_map, resource_hostname, resource_name);

        REQUIRE(in_host_list == false);
        REQUIRE(new_vault_path_optional.has_value() == false);
    }*/

    SECTION("at beginning of host list no path")
    {
        irods::plugin_property_map prop_map;
        prop_map.set<std::string>("HOST_LIST", "myhost,host2:/host2/path,host3");

        bool in_host_list;
        std::optional<std::string> new_vault_path_optional;

        std::tie(in_host_list, new_vault_path_optional) =
            get_detached_mode_vault_path(prop_map, resource_hostname, resource_name);

        REQUIRE(in_host_list == true);
        REQUIRE(new_vault_path_optional.has_value() == false);
    }

    SECTION("at end of host list no path")
    {
        irods::plugin_property_map prop_map;
        prop_map.set<std::string>("HOST_LIST", "host2:/host2/path,host3,myhost");

        bool in_host_list;
        std::optional<std::string> new_vault_path_optional;

        std::tie(in_host_list, new_vault_path_optional) =
            get_detached_mode_vault_path(prop_map, resource_hostname, resource_name);

        REQUIRE(in_host_list == true);
        REQUIRE(new_vault_path_optional.has_value() == false);
    }

    SECTION("in middle of host list no path")
    {
        irods::plugin_property_map prop_map;
        prop_map.set<std::string>("HOST_LIST", "host2:/host2/path,myhost,host3");

        bool in_host_list;
        std::optional<std::string> new_vault_path_optional;

        std::tie(in_host_list, new_vault_path_optional) =
            get_detached_mode_vault_path(prop_map, resource_hostname, resource_name);

        REQUIRE(in_host_list == true);
        REQUIRE(new_vault_path_optional.has_value() == false);
    }

    SECTION("at beginning of host list with path")
    {
        irods::plugin_property_map prop_map;
        prop_map.set<std::string>("HOST_LIST", "myhost:/my/path,host2:/host2/path,host3");

        bool in_host_list;
        std::optional<std::string> new_vault_path_optional;

        std::tie(in_host_list, new_vault_path_optional) =
            get_detached_mode_vault_path(prop_map, resource_hostname, resource_name);

        REQUIRE(in_host_list == true);
        REQUIRE(new_vault_path_optional.has_value() == true);
        REQUIRE(new_vault_path_optional.value() == "/my/path");
    }

    SECTION("at end of host list with path")
    {
        irods::plugin_property_map prop_map;
        prop_map.set<std::string>("HOST_LIST", "host2:/host2/path,host3,myhost:/my/path");

        bool in_host_list;
        std::optional<std::string> new_vault_path_optional;

        std::tie(in_host_list, new_vault_path_optional) =
            get_detached_mode_vault_path(prop_map, resource_hostname, resource_name);

        REQUIRE(in_host_list == true);
        REQUIRE(new_vault_path_optional.has_value() == true);
        REQUIRE(new_vault_path_optional.value() == "/my/path");
    }

    SECTION("in middle of host list with path")
    {
        irods::plugin_property_map prop_map;
        prop_map.set<std::string>("HOST_LIST", "host2:/host2/path,myhost:/my/path,host3");

        bool in_host_list;
        std::optional<std::string> new_vault_path_optional;

        std::tie(in_host_list, new_vault_path_optional) =
            get_detached_mode_vault_path(prop_map, resource_hostname, resource_name);

        REQUIRE(in_host_list == true);
        REQUIRE(new_vault_path_optional.has_value() == true);
        REQUIRE(new_vault_path_optional.value() == "/my/path");
    }
}

