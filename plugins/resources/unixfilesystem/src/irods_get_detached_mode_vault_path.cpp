#include "irods/private/irods_get_detached_mode_vault_path.hpp"

#include "irods/filesystem/path.hpp"
#include "irods/irods_error.hpp"
#include "irods/irods_logger.hpp"
#include "irods/irods_lookup_table.hpp"

#include <vector>
#include <sstream>
#include <string>
#include <tuple>

const std::string HOST_LIST("HOST_LIST");

static std::vector<std::string> split(const std::string str, char delim)
{
    std::vector<std::string> result;
    std::istringstream ss{str};
    std::string token;
    while (std::getline(ss, token, delim)) {
        if (!token.empty()) {
            result.push_back(token);
        }
    }
    return result;
}

// returns a tuple with the following:
//   bool - true iff the resource_hostname is in HOST_LIST string
//   std::optional<std::string> - the new vault path if it is defined
std::tuple<bool, std::optional<std::string>> get_detached_mode_vault_path(
        irods::plugin_property_map& prop_map,
        const std::string& resource_hostname,
        const std::string& resource_name)
{
    using logger = irods::experimental::log;

    bool in_host_list = false;
    std::string new_vault_path;

    std::string host_list_str;

    irods::error ret = prop_map.get< std::string >(HOST_LIST, host_list_str);
    if (!ret.ok()) {

        // no HOST_LIST parameter, all hosts assumed to be able to handle request
        // and original vault path used for all hosts
        in_host_list = true;
        return std::make_tuple(in_host_list, std::nullopt);
    }

    // have a HOST_LIST parameter

    // split parameter by delimiter (,)
    char delimiter = ',';
    std::string token;

    std::string resource_hostname_with_colon = resource_hostname + ":";

    std::vector<std::string> tokens = split(host_list_str, delimiter);

    for (std::string& token : tokens) {


        // see if this token begins with our resource location but with no path
        if (token == resource_hostname) {

            // we have our location but no vault path, in host but do not update vault path
            in_host_list = true;
            return std::make_tuple(in_host_list, std::nullopt);
        }

        if (token.starts_with(resource_hostname_with_colon)) {

            in_host_list = true;
            std::string new_vault_path = token.substr(resource_hostname_with_colon.length(), token.find(delimiter));

            // make sure vault path is absolute
            irods::experimental::filesystem::path p(new_vault_path);
            if (!p.is_absolute()) {
                // log a warning but continue
                logger::resource::warn("[resource_name={}] Detached mode vault path ({}) is not absolute.  "
                        "Resource will not be considered in the host list.",
                        resource_name, resource_hostname);
                in_host_list = false;
                return std::make_tuple(in_host_list, std::nullopt);

            }
            return std::make_tuple(in_host_list, new_vault_path);
        }

    }

    return std::make_tuple(in_host_list, std::nullopt);

} // get_detached_mode_vault_path
