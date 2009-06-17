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


namespace {


    class ADOWriter;
    FSL_EXPORT const class ADOInterface : public fostlib::sql_driver {
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
        using fostlib::sql_driver::mangle;
        fostlib::sql::statement mangle( const fostlib::sql::table_name &name ) const {
            return fostlib::sql::statement( L"[" ) + fostlib::sql::statement( name.underlying() ) + fostlib::sql::statement( L"]" );
        }
        fostlib::sql::statement mangle( const fostlib::sql::column_name &name ) const {
            return fostlib::sql::statement( L"[" ) + fostlib::sql::statement( name.underlying() ) + fostlib::sql::statement( L"]" );
        }
    } c_ado_interface;


    class ADOReader : public fostlib::dbinterface::read {
    public:
        ADOReader( fostlib::dbconnection &d )
        : read( d ) {
        }
        ~ADOReader() {
        }

        boost::shared_ptr< fostlib::dbinterface::recordset > query( const fostlib::meta_instance &item, const fostlib::json &key ) const;
        boost::shared_ptr< fostlib::dbinterface::recordset > query( const fostlib::sql::statement &cmd ) const;

        boost::shared_ptr< fostlib::dbinterface::write > writer();
    };


    class ADOWriter : public fostlib::sql_driver::write {
        mutable ADODB::_ConnectionPtr m_cnx;
        mutable bool m_commit;
    public:
        ADOWriter( fostlib::dbconnection &dbc, fostlib::dbinterface::read &reader, bool start_transaction )
        : write( reader ), m_commit( start_transaction ) {
            ADODB::_ConnectionPtr cnx;
            cnx.CreateInstance( __uuidof( ADODB::Connection ) );
            cnx->CommandTimeout = fostlib::setting< int >::value( L"Database", L"WriteCommandTimeout" );
            cnx->Open( dbc.configuration()[ L"read" ].get< fostlib::string >().value().c_str(), L"", L"", ADODB::adConnectUnspecified );
            if ( start_transaction )
                cnx->BeginTrans();
            m_cnx = cnx;
        }
        ~ADOWriter() {
            if ( m_cnx ) {
                m_cnx = NULL;
                throw fostlib::exceptions::transaction_fault( L"The DBInterface::Write connection must be committed or rolled back" );
            }
        }

        void drop_table( const fostlib::meta_instance &definition );
        void drop_table( const fostlib::string &table );

        void insert( const fostlib::instance &object );

        ADODB::_RecordsetPtr recordset( const fostlib::sql::statement &cmd ) {
            try {
                if ( !m_cnx )
                    throw fostlib::exceptions::transaction_fault( L"This transaction has already been used" );
                _variant_t res; ADODB::_RecordsetPtr rs;
                rs = m_cnx->Execute( cmd.underlying().c_str(), &res, ADODB::adOptionUnspecified );
                m_commit |= fostlib::coerce< long >( res ) > 0;
                return rs;
            } catch ( _com_error &c ) {
                m_cnx = NULL;
                throw fostlib::exceptions::com_error( c, L"During execute" );
            } catch ( ... ) {
                m_cnx = NULL;
                throw fostlib::exceptions::transaction_fault( L"Unknown error during execute" );
            }
       }
        void execute( const fostlib::sql::statement &cmd ) {
            recordset( cmd );
        }

        void commit() {
            if ( !m_cnx )
                throw fostlib::exceptions::transaction_fault( L"This transaction has already been used" );
	        try {
                if ( m_commit )
                    m_cnx->CommitTrans();
                m_cnx = NULL;
                if ( fostlib::dbconnection::c_commitCount.value() )
                    c_ado_interface.next_id( m_connection, fostlib::dbconnection::c_commitCountDomain.value() );
	        } catch ( _com_error &c ) {
                m_cnx = NULL;
                throw fostlib::exceptions::com_error( c, L"Error during CommitTrans()" );
	        } catch ( ... ) {
                m_cnx = NULL;
                throw fostlib::exceptions::transaction_fault( L"Unknown error during transaction commit" );
	        }
        }
        void rollback() {
            try {
                m_cnx = NULL;
            } catch ( _com_error &c ) {
                throw fostlib::exceptions::com_error( c, L"Error during RollbackTrans()" );
            }
        }
    };


    class RSInterface : public fostlib::dbinterface::recordset {
        ADODB::_ConnectionPtr m_cnx;
        ADODB::_RecordsetPtr m_rs;
        std::map< fostlib::string, std::size_t > m_fields;
        mutable std::vector< fostlib::nullable< fostlib::json > > m_values;
    public:
        RSInterface( const fostlib::dbconnection &dbc, const fostlib::sql::statement &cmd, ADODB::_RecordsetPtr rs )
        : recordset( cmd ), m_rs( rs ) {
            // Build field list so we don't need to use the ADO mappings
            for ( long c( 0 ); c < m_rs->Fields->Count; ++c )
	            m_fields[ fostlib::coerce< fostlib::string >( m_rs->Fields->Item[ c ]->Name ) ] = c;
            m_values = std::vector< fostlib::nullable< fostlib::json > >( m_fields.size() );
        }
        RSInterface( const fostlib::dbconnection &dbc, const fostlib::sql::statement &cmd )
        : recordset( cmd ) {
            fostlib::string rw( L"read" );
            try {
	            m_cnx.CreateInstance( __uuidof( ADODB::Connection ) );
                m_cnx->CommandTimeout = fostlib::setting< int >::value( L"Database", L"ReadCommandTimeout" );
                m_cnx->Open( dbc.configuration()[ L"read" ].get< fostlib::string >().value().c_str(), L"", L"", ADODB::adConnectUnspecified );
	            _variant_t res;
	            m_rs = m_cnx->Execute( cmd.underlying().c_str(), &res, ADODB::adOptionUnspecified );
	            // Build field list so we don't need to use the ADO mappings
	            for ( long c( 0 ); c < m_rs->Fields->Count; ++c )
		            m_fields[ fostlib::coerce< fostlib::string >( m_rs->Fields->Item[ c ]->Name ) ] = c;
                m_values = std::vector< fostlib::nullable< fostlib::json > >( m_fields.size() );
            } catch ( fostlib::exceptions::exception &e ) {
	            e.info() << L"Failure in execute on database connection\n" <<
                        rw << L" connection\n" << cmd << '\n' << fostlib::json::unparse( dbc.configuration()[ L"read" ], true ) << std::endl;
	            throw;
            } catch ( _com_error &c ) {
	            if ( cmd.empty() )
		            throw fostlib::exceptions::com_error( c );
	            else
		            throw fostlib::exceptions::com_error( c, rw + L" connection\n" + cmd.underlying() );
            }
	    }
	    ~RSInterface()
	    try {
		    // No where in the help does it explicitly state that a recordset is closed for you on
		    // release of the COM pointer. Indeed the C++ examples show "ADODB::Recordset::Close()" and
		    // "ADODB::Connection::Close()" being called. So taking my lead from there... Also the help
		    // strongly implies that you get handle leaks if you don't call "ADODB::*::Close()".
		    // Maybe not calling close can cause instability?
		    // We only need to close read recordsets. Write ones are in out system, never read from, so are never opened, therefore don't need to be closed.
		    // Indeed closing an unopened recordset will throw a COM exception. (I tried...)
		    m_rs->Close();
		    m_cnx->Close();
		    m_rs = ADODB::_RecordsetPtr();
		    m_cnx = ADODB::_ConnectionPtr();
	    } catch ( _com_error &c ) {
            throw fostlib::exceptions::com_error( c, L"Closing ADO recordset" );
	    }

        bool eof() const {
	        try {
		        return m_rs->adoEOF ? true : false;
	        } catch ( _com_error &c ) {
                throw fostlib::exceptions::com_error( c, L"Checking for EOF on recordset" );
	        }
        }

        void moveNext() {
        	try {
		        m_rs->MoveNext();
                m_values = std::vector< fostlib::nullable< fostlib::json > >( m_fields.size() );
	        } catch( _com_error &c ) {
                throw fostlib::exceptions::com_error( c, L"Moving to next record" );
	        }
        }

        std::size_t fields() const {
            return m_values.size();
        }

        const fostlib::string &name( std::size_t f ) const {
	        for ( std::map< fostlib::string, std::size_t >::const_iterator i( m_fields.begin() ); i != m_fields.end(); ++i )
		        if ( (*i).second == f )
			        return (*i).first;
	        throw fostlib::exceptions::unexpected_eof( L"Field number is beyond number of actual fields" );
        }

        const fostlib::json &field( std::size_t i ) const {
	        if ( i >= m_values.size() )
                throw fostlib::exceptions::out_of_range< std::size_t >( L"Ordinal number is too high for the number of values in the recordset", 0, m_values.size(), i );
	        try {
                if ( m_values[ i ].isnull() ) {
                    _variant_t value( m_rs->Fields->Item[ _variant_t( long( i ) ) ]->Value );
                    if ( value.vt == VT_BOOL ) {
                        fostlib::nullable< bool > f( fostlib::coerce< bool >( value ) );
                        m_values[ i ] = f.isnull() ? fostlib::json() : fostlib::json( f.value() );
                    } else {
                        fostlib::nullable< fostlib::string > f( fostlib::coerce< fostlib::nullable< fostlib::string > >( value ) );
                        m_values[ i ] = f.isnull() ? fostlib::json() : fostlib::json( f.value() );
                    }
                }
		        return m_values[ i ].value();
	        } catch ( _com_error &c ) {
                throw fostlib::exceptions::com_error( c, L"Fetching database recordset ordinal: " + fostlib::coerce< fostlib::string >( i ) );
	        }
        }
        const fostlib::json &field( const fostlib::string &name ) const {
            std::map< fostlib::string, std::size_t >::const_iterator pos( m_fields.find( name ) );
	        if ( pos == m_fields.end() )
                throw fostlib::exceptions::null( L"Cannot find the field", name );
	        else {
		        try {
			        return field( (*pos).second );
                } catch ( fostlib::exceptions::com_error &e ) {
                    e.info() << L"Fetching the database field: " << name << std::endl;
			        throw;
		        }
	        }
        }

        fostlib::json to_json() const {
            throw fostlib::exceptions::not_implemented( L"json RSInterface::to_json() const" );
        }
    };


}


/*
    ADOInterface
*/


void ADOInterface::create_database( fostlib::dbconnection &dbc, const fostlib::string &name ) const {
    ADOReader reader( dbc );
    ADOWriter writer( dbc, reader, false );
    writer.execute( fostlib::sql::statement( L"CREATE DATABASE \"" + name + L"\"" ) );
}

void ADOInterface::drop_database( fostlib::dbconnection &dbc, const fostlib::string &name ) const {
    ADOReader reader( dbc );
    ADOWriter writer( dbc, reader, false );
    writer.execute( fostlib::sql::statement( L"DROP DATABASE \"" + name + L"\"" ) );
}


int64_t ADOInterface::next_id( fostlib::dbconnection &dbc, const fostlib::string &counter ) const {
    fostlib::sql::statement sql =fostlib::sql::statement(
        L"BEGIN TRAN\n"
        L"SET NOCOUNT ON\n"
        L"UPDATE FSLib_Domain SET nextID=nextID+1 WHERE id=(SELECT id FROM FSLib_Object WHERE type_id=7 AND displayName=" ) + mangle( counter ) + fostlib::sql::statement( L")\n"
        L"SET NOCOUNT OFF\n"
        L"SELECT (nextID - 1) AS next FROM FSLib_Domain WHERE id=(SELECT id FROM FSLib_Object WHERE type_id=7 AND displayName=" ) + mangle( counter ) + fostlib::sql::statement( L")\n"
        L"COMMIT TRAN"
    );
    // Use the read connection as it has the right API
    return fostlib::coerce< int64_t >( reader( dbc )->query( sql )->field( 0 ) );
}

int64_t ADOInterface::current_id( fostlib::dbconnection &dbc, const fostlib::string &counter ) const {
    try {
        fostlib::sql::statement sql = fostlib::sql::statement( L"SELECT (nextID - 1) AS cur FROM FSLib_Domain WHERE id=(SELECT id FROM FSLib_Object WHERE type_id=7 AND displayName=" ) + mangle( counter ) + fostlib::sql::statement( L")" );
        RSInterface rs( dbc, sql );
        if ( rs.eof() ) throw fostlib::exceptions::null( L"No ID number found for domain" );
        return fostlib::coerce< int64_t >( rs.field( 0 ) );
    } catch ( fostlib::exceptions::exception &e ) {
        e.info() << L"Fetching current ID for domain " << counter << std::endl;
        throw;
    }
}
void ADOInterface::used_id( fostlib::dbconnection &dbc, const fostlib::string &counter, int64_t value ) const {
    fostlib::sql::statement number( mangle( value ) );
    ADOWriter writer( dbc, *reader( dbc ), true );
    writer.execute( fostlib::sql::statement( L"UPDATE FSLib_Domain SET nextID=" ) + number + fostlib::sql::statement( L" + 1 WHERE nextID < " ) + number + fostlib::sql::statement( L" + 1 AND id=(SELECT id FROM FSLib_Object WHERE type_id=7 AND displayName=" ) + mangle( counter ) + fostlib::sql::statement( L")" ) );
    writer.commit();
}


boost::shared_ptr< fostlib::dbinterface::read > ADOInterface::reader( fostlib::dbconnection &dbc ) const {
    return boost::shared_ptr< fostlib::dbinterface::read >( new ADOReader( dbc ) );
}


/*
    ADOReader
*/


boost::shared_ptr< fostlib::dbinterface::recordset > ADOReader::query( const fostlib::meta_instance &item, const fostlib::json &key ) const {
    throw fostlib::exceptions::not_implemented( L"boost::shared_ptr< recordset > query( const meta_instance &item, const json &key ) const" );
}


boost::shared_ptr< fostlib::dbinterface::recordset > ADOReader::query( const fostlib::sql::statement &command ) const {
    return boost::shared_ptr< fostlib::dbinterface::recordset >( new RSInterface( m_connection, command ) );
}


boost::shared_ptr< fostlib::dbinterface::write > ADOReader::writer() {
    return boost::shared_ptr< fostlib::dbinterface::write >( new ADOWriter( m_connection, *this, true ) );
}


/*
    ADOWriter
*/


void ADOWriter::drop_table(class fostlib::meta_instance const &) {
    throw fostlib::exceptions::not_implemented( L"ADOWriter::drop_table(class fostlib::meta_instance const &)" );
}
void ADOWriter::insert(class fostlib::instance const &) {
    throw fostlib::exceptions::not_implemented( L"ADOWriter::insert(class fostlib::instance const &)" );
}

void ADOWriter::drop_table( const fostlib::string &/*table*/ ) {
    throw fostlib::exceptions::not_implemented( L"void ADOWriter::drop_table( const wstring &table ) const" );
}
