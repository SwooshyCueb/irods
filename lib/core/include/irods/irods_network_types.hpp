#ifndef __IRODS_NETWORK_TYPES_HPP__

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
// network plugin pointer type
    class network;
    typedef std::shared_ptr< network > network_ptr;

// =-=-=-=-=-=-=-
// fwd decl of network manager for fco resolve
    class network_manager;

}; // namespace

#define __IRODS_NETWORK_TYPES_HPP__
#endif // __IRODS_NETWORK_TYPES_HPP__



