#ifndef __IRODS_AUTH_TYPES_HPP__

// =-=-=-=-=-=-=-
// Boost Includes
#include <boost/any.hpp>

// =-=-=-=-=-=-=-
#include "irods/irods_plugin_base.hpp"
#include "irods/irods_lookup_table.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include "irods/rcConnect.h"

// =-=-=-=-=-=-=-
// stl includes
#include <memory>

namespace irods {
// =-=-=-=-=-=-=-
// auth plugin pointer type
    class auth;
    typedef std::shared_ptr< auth > auth_ptr;

// =-=-=-=-=-=-=-
// fwd decl of network manager for fco resolve
    class auth_manager;

}; // namespace

#define __IRODS_AUTH_TYPES_HPP__
#endif // __IRODS_AUTH_TYPES_HPP__



