#ifndef __IRODS_FIRST_CLASS_OBJECT_HPP__
#define __IRODS_FIRST_CLASS_OBJECT_HPP__

// =-=-=-=-=-=-=-
#include "irods/irods_log.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include "irods/rcConnect.h"

// =-=-=-=-=-=-=-
// stl includes
#include <map>
#include <memory>

namespace irods {

    class plugin_base;
    typedef std::shared_ptr<plugin_base>    plugin_ptr;
    typedef std::map<std::string,std::string> rule_engine_vars_t;
    // =-=-=-=-=-=-=-
    // base class for all object types
    class first_class_object {
        public:
            // =-=-=-=-=-=-=-
            // Constructors
            first_class_object() {};

            // =-=-=-=-=-=-=-
            // Destructor
            virtual ~first_class_object() {};

            // =-=-=-=-=-=-=-
            // plugin resolution operators
            virtual error resolve(
                const std::string&, // plugin interface
                plugin_ptr& ) = 0;  // resolved plugin

            // =-=-=-=-=-=-=-
            // accessor for rule engine variables
            virtual error get_re_vars( rule_engine_vars_t& ) = 0;

    }; // class first_class_object

    /// =-=-=-=-=-=-=-
    /// @brief shared pointer to first_class_object
    typedef std::shared_ptr< first_class_object > first_class_object_ptr;

}; // namespace irods

#endif // __IRODS_FIRST_CLASS_OBJECT_HPP__



