/*
    Copyright 1999-2009, Felspar Co Ltd. http://fost.3.felspar.com/
    Distributed under the Boost Software License, Version 1.0.
    See accompanying file LICENSE_1_0.txt or copy at
        http://www.boost.org/LICENSE_1_0.txt
*/


#include <fost/db-driver-sql>
#import <c:\program files\common files\system\ado\msado15.dll> rename( "EOF", "adoEOF" )
#include <fost/coerce/win.hpp>

#include <fost/exception/not_implemented.hpp>
#include <fost/exception/transaction_fault.hpp>
#include <fost/exception/unexpected_eof.hpp>


class ADOInterface : public fostlib::sql_driver {
public:
    ADOInterface()
    : sql_driver( L"ado" ) {
    }

    void create_database( fostlib::dbconnection &dbc, const fostlib::string &name ) const;
    void drop_database( fostlib::dbconnection &dbc, const fostlib::string &name ) const;

    int64_t next_id( fostlib::dbconnection &dbc, const fostlib::string &counter ) const;
    int64_t current_id( fostlib::dbconnection &dbc, const fostlib::string &counter ) const;
    void used_id( fostlib::dbconnection &dbc, const fostlib::string &counter, int64_t value ) const;

    boost::shared_ptr< fostlib::dbinterface::read > reader( fostlib::dbconnection &dbc ) const;

protected:
    ADOInterface( const fostlib::string &name )
    : sql_driver( name ) {
    }

    using fostlib::sql_driver::mangle;
    fostlib::sql::statement mangle( const fostlib::sql::table_name &name ) const {
        return fostlib::sql::statement( L"[" ) + fostlib::sql::statement( name.underlying() ) + fostlib::sql::statement( L"]" );
    }
    fostlib::sql::statement mangle( const fostlib::sql::column_name &name ) const {
        return fostlib::sql::statement( L"[" ) + fostlib::sql::statement( name.underlying() ) + fostlib::sql::statement( L"]" );
    }
};
