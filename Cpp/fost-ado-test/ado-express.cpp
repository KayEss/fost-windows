/*
    Copyright 2008-2009, Felspar Co Ltd. http://fost.3.felspar.com/
    Distributed under the Boost Software License, Version 1.0.
    See accompanying file LICENSE_1_0.txt or copy at
        http://www.boost.org/LICENSE_1_0.txt
*/


#include <fost/test>
#include <fost/db>

#include <fost/exception/transaction_fault.hpp>

#include <boost/assign/list_inserter.hpp>


FSL_TEST_SUITE( db_ado_express );


FSL_TEST_FUNCTION( connect_database ) {
    fostlib::dbconnection dbc( L"ado/driver={SQL server}; server=.\\SQLEXPRESS;" );
}


FSL_TEST_FUNCTION( normal_statements ) {
    const fostlib::setting< bool > commit_count( L"fost-ado-smoke-test/ado-express.cpp", fostlib::dbconnection::c_commitCount, false );

    {
        fostlib::dbconnection dbc( L"ado/driver={SQL server}; server=.\\SQLEXPRESS;", L"ado/driver={SQL server}; server=.\\SQLEXPRESS;" );

        fostlib::recordset rs1( dbc.query( fostlib::sql::statement( L"SELECT 1 WHERE 1=0" ) ) );
        FSL_CHECK( rs1.eof() );
        FSL_CHECK_EQ( rs1.fields(), 1 );
        FSL_CHECK_EQ( rs1.command(), fostlib::sql::statement( L"SELECT 1 WHERE 1=0" ) );

        fostlib::recordset rs2( dbc.query( fostlib::sql::statement( L"SELECT 1234 AS c0" ) ) );
        FSL_CHECK( !rs2.eof() );
        FSL_CHECK_EQ( fostlib::coerce< int >( rs2.field( 0 ) ), 1234 );
        FSL_CHECK_EQ( fostlib::coerce< int >( rs2.field( L"c0" ) ), 1234 );
        FSL_CHECK_EQ(
            fostlib::json::unparse( rs2.to_json(), true ),
            fostlib::json::unparse( fostlib::json::parse( L"{\"c0\":\"1234\"}" ), true )
        );

        fostlib::recordset databases( dbc.query( fostlib::sql::statement( L"SELECT dbid FROM sysdatabases WHERE name='FSL_Test'" ) ) );

        if ( !databases.eof() )
            dbc.drop_database( L"FSL_Test" );

        {
            try {
                dbc.create_database( L"FSL_TEST" );
            } catch ( fostlib::exceptions::exception &e ) {
                e.info() << L"Whilst performing CREATE DATABASE" << std::endl;
                throw;
            }
            FSL_CHECK( !dbc.in_transaction() );
            try {
                fostlib::dbconnection dbc( L"ado/driver={SQL server}; server=.\\SQLEXPRESS; database=FSL_Test", L"ado/driver={SQL server}; server=.\\SQLEXPRESS; database=FSL_Test" );

                fostlib::meta_instance test( L"Test" );
                test
                    .primary_key( L"id", L"integer" )
                    .field( L"name", L"varchar", false, 128 );

                fostlib::dbtransaction transaction( dbc );
                transaction.create_table( test );
                transaction.commit();
            } catch ( fostlib::exceptions::exception &e ) {
                e.info() << L"Whist creating a table from a meta_instance" << std::endl;
                throw;
            }
        }
    }
}


FSL_TEST_FUNCTION( select_safeguards ) {
    fostlib::dbconnection dbc( L"ado/driver={SQL server}; server=.\\SQLEXPRESS;" );

    FSL_CHECK_EXCEPTION( fostlib::recordset( dbc.query( fostlib::sql::statement( L"SELECT MAX( not_a_column ) FROM Not_a_table" ) ) ), fostlib::exceptions::com_error& );

    fostlib::recordset rs1( dbc.query( fostlib::sql::statement( L"SELECT 1 WHERE 1=0" ) ) );
    FSL_CHECK_EXCEPTION( fostlib::coerce< int >( rs1.field( 0 ) ), fostlib::exceptions::com_error& );
}


FSL_TEST_FUNCTION( transaction_safeguards_1_setup ) {
    const fostlib::setting< bool > commit_count( L"fost-ado-smoke-test/ado-express.cpp", fostlib::dbconnection::c_commitCount, false );
    fostlib::dbconnection dbc( L"ado/driver={SQL server}; server=.\\SQLEXPRESS; database=FSL_Test" );

    fostlib::dbtransaction transaction( dbc );
    transaction.execute( fostlib::sql::statement( L"DELETE FROM Test" ) );
    transaction.commit();
}

FSL_TEST_FUNCTION( transaction_safeguards_2_reuse_transaction ) {
    const fostlib::setting< bool > commit_count( L"fost-ado-smoke-test/ado-express.cpp", fostlib::dbconnection::c_commitCount, false );
    fostlib::dbconnection dbc( L"ado/driver={SQL server}; server=.\\SQLEXPRESS; database=FSL_Test" );

    { // Check we can't reuse a committed transaction
        fostlib::dbtransaction transaction( dbc );

        FSL_CHECK_EXCEPTION( fostlib::dbtransaction t2( dbc ), fostlib::exceptions::transaction_fault& );

        transaction.execute( fostlib::sql::statement( L"INSERT INTO Test VALUES (1, 'Hello')" ) );
        transaction.commit();
        FSL_CHECK_EXCEPTION( transaction.execute( fostlib::sql::statement( L"INSERT INTO Test VALUES (2, 'Hello')" ) ), fostlib::exceptions::transaction_fault& );
    }
    FSL_CHECK_EQ( fostlib::coerce< int >( dbc.query( fostlib::sql::statement( L"SELECT COUNT(id) FROM Test" ) ).field( 0 ) ), 1 );
}

FSL_TEST_FUNCTION( transaction_safeguards_3_duplicate_key ) {
    const fostlib::setting< bool > commit_count( L"fost-ado-smoke-test/ado-express.cpp", fostlib::dbconnection::c_commitCount, false );
    fostlib::dbconnection dbc( L"ado/driver={SQL server}; server=.\\SQLEXPRESS; database=FSL_Test" );

    { // Check that a duplicate key results in an error
        fostlib::dbtransaction transaction( dbc );
        FSL_CHECK_EXCEPTION( transaction.execute( fostlib::sql::statement( L"INSERT INTO Test VALUES (1, 'Hello')" ) ), fostlib::exceptions::com_error& );
    }
    FSL_CHECK_EQ( fostlib::coerce< int >( dbc.query( fostlib::sql::statement( L"SELECT COUNT(id) FROM Test" ) ).field( 0 ) ), 1 );
}

FSL_TEST_FUNCTION( transaction_safeguards_4_dropped_transaction ) {
    const fostlib::setting< bool > commit_count( L"fost-ado-smoke-test/ado-express.cpp", fostlib::dbconnection::c_commitCount, false );
    fostlib::dbconnection dbc( L"ado/driver={SQL server}; server=.\\SQLEXPRESS; database=FSL_Test" );

    { // Check that a dropped transaction is aborted
        fostlib::dbtransaction transaction( dbc );
        transaction.execute( fostlib::sql::statement( L"INSERT INTO Test VALUES (2, 'Goodbye')" ) );
    }
    FSL_CHECK_EQ( fostlib::coerce< int >( dbc.query( fostlib::sql::statement( L"SELECT COUNT(id) FROM Test" ) ).field( 0 ) ), 1 );
}


// SQL Express isolates through locks so we can't test this without waiting for a long time out
//{ // Check that a transaction is isolated
//    fostlib::dbtransaction transaction( dbc );
//    transaction.execute( L"INSERT INTO Test VALUES (2, 'Goodbye')" );

//    fostlib::dbconnection cnx( L"ado/driver={SQL server}; server=.\\SQLEXPRESS; database=FSL_Test" );
//    FSL_CHECK_EQ( fostlib::coerce< long >( cnx.recordset( L"SELECT COUNT(id) FROM Test" ).field( 0 ) ), 1 );
//    transaction.commit();
//}
//FSL_CHECK_EQ( fostlib::coerce< long >( dbc.recordset( L"SELECT COUNT(id) FROM Test" ).field( 0 ) ), 2 );

