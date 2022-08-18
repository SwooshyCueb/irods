#ifndef _IRODS_GET_DETACHED_MODE_VAULT_PATH_HPP_
#define _IRODS_GET_DETACHED_MODE_VAULT_PATH_HPP_

#include "irods/irods_lookup_table.hpp"

#include <optional>
#include <string>
#include <tuple>

std::tuple<bool, std::optional<std::string>> get_detached_mode_vault_path(
        irods::plugin_property_map& prop_map,
        const std::string& resource_hostname,
        const std::string& resource_name);

#endif
