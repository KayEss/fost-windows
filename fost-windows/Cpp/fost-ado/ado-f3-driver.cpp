/*
    Copyright 1999-2009, Felspar Co Ltd. http://fost.3.felspar.com/
    Distributed under the Boost Software License, Version 1.0.
    See accompanying file LICENSE_1_0.txt or copy at
        http://www.boost.org/LICENSE_1_0.txt
*/


#include "ado.hpp"


namespace {


    FSL_EXPORT const class ADOF3 : public ADOInterface {
    public:
        ADOF3()
        : ADOInterface( L"ado.f3" ) {
        }

    protected:
        fostlib::sql::statement mangle( const fostlib::string &s ) const;
    } c_adof3_interface;


    inline std::pair< bool, fostlib::sql::statement > fostMangle( const fostlib::string &field ) {
	    fostlib::string s;
	    bool extended( false );

	    for ( fostlib::string::const_iterator i( field.begin() ); i != field.end(); ++i ) {
		    if ( *i > 0x7f && !extended ) extended = true;
		    if ( *i == L'\'' )
			    s += L"&s";
		    else if ( *i == L'"' )
			    s += L"&d";
		    else if ( *i == L'\t' )
			    s += L"&t";
		    else if ( *i == L'\n' )
			    s += L"&n";
		    else if ( *i == L'\r' )
			    s += L"&r";
		    else if ( *i == L'&' )
			    s += L"&&";
		    else
			    s += *i;
	    }

        return std::make_pair( extended, fostlib::sql::statement( s ) );
    }


}


fostlib::sql::statement ADOF3::mangle( const fostlib::string &s ) const {
    std::pair< bool, fostlib::sql::statement > mangled( ::fostMangle( s ) );
	return fostlib::sql::statement( mangled.first ? L"N\'" : L"\'" ) + mangled.second + fostlib::sql::statement( L"\'" );
}
