#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

#include <sqlhdr.h>
#include <sqliapi.h>
#include <sqltypes.h>

#include "lua.h"
#include "lauxlib.h"

#include "luasql.h"

#define LUASQL_ENVIRONMENT_INFORMIX "INFORMIX environment"
#define LUASQL_CONNECTION_INFORMIX "INFORMIX connection"
#define LUASQL_CURSOR_INFORMIX "INFORMIX cursor"

#define ENV_INFORMIX_SVR "INFORMIXSERVER"
#define MAX_NAME_LENGTH  128

typedef struct {
	short	closed;
	char	*old_env;			/* point to env str in u area */
	char	curr_env[MAX_NAME_LENGTH+32];	/* string to set env */
	int		conn_cnt;			/* total connection count */
} env_data;

typedef struct {
	short	closed;
	int		env;                /* reference to environment */
	char	conn_name[MAX_NAME_LENGTH];
	int		stmt_cnt;			/* total sql statement count */
	int		auto_commit;
	int		auto_begin;
	ifx_sqlca_t	conn_sqlca;
} conn_data;

typedef struct {
	short	closed;
	int		conn;               /* reference to connection */
	int		colnames, coltypes; /* reference to column information tables */
	char	cur_name[MAX_NAME_LENGTH];
	ifx_sqlda_t *cur_sqlda;
	char	*buf;				/* buffer to put fetch data */
	int2	*indicators;		/* buffer for the indicators */
} cur_data;

LUASQL_API int luaopen_luasql_informix (lua_State *L);

/*
** Check for valid environment.
*/
static env_data *getenvironment (lua_State *L) {
	env_data *env = (env_data *)luaL_checkudata(L, 1, LUASQL_ENVIRONMENT_INFORMIX);
	luaL_argcheck(L, env != NULL, 1, "environment expected");
	luaL_argcheck(L, !env->closed, 1, "environment is closed");
	return env;
}


/*
** Check for valid connection.
*/
static conn_data *getconnection (lua_State *L) {
	conn_data *conn = (conn_data *)luaL_checkudata(L, 1, LUASQL_CONNECTION_INFORMIX);
	luaL_argcheck(L, conn != NULL, 1, "connection expected");
	luaL_argcheck(L, !conn->closed, 1, "connection is closed");
	return conn;
}


/*
** Check for valid cursor.
*/
static cur_data *getcursor (lua_State *L) {
	cur_data *cur = (cur_data *)luaL_checkudata(L, 1, LUASQL_CURSOR_INFORMIX);
	luaL_argcheck(L, cur != NULL, 1, "cursor expected");
	luaL_argcheck(L, !cur->closed, 1, "cursor is closed");
	return cur;
}

/*
** Get conn data from ref
*/
static conn_data *getconnfromref (lua_State *L, int ref) {
	conn_data *conn = NULL;
	lua_rawgeti(L, LUA_REGISTRYINDEX, ref);
	conn = (conn_data *)luaL_checkudata(L, -1, LUASQL_CONNECTION_INFORMIX);
	lua_pop(L, 1);
	return conn;
}


/*
** Get env data from ref
*/
static env_data *getenvfromref (lua_State *L, int ref) {
	env_data *env = NULL;
	lua_rawgeti(L, LUA_REGISTRYINDEX, ref);
	env = (env_data *)luaL_checkudata(L, -1, LUASQL_ENVIRONMENT_INFORMIX);
	lua_pop(L, 1);
	return env;
}


/*
** switch env 
*/
inline static int set_env (env_data *env) {
	if (strlen(env->curr_env) != 0) {
		return putenv(env->curr_env);
	}
	return 0;
}

inline static int restore_env (env_data *env) {
	if ((env->old_env != NULL)&&(strlen(env->curr_env)) != 0) {
		return putenv(env->old_env);
	}
	return 0;
}


/*
** switch connection
*/
inline static void set_conn (lua_State *L, conn_data *conn) {
	sqli_connect_set(0, conn->conn_name, 0);
}


/*
** Push the value of #i field of #tuple row.
*/
static void pushvalue (lua_State *L, int2 *ind, int type, char *data, long len) {
	int i;
	char str_num[64];
	char c_data;
	long l_data;
	double d_data;

	if (*ind == -1) {
		lua_pushnil(L);
		return;
	}
	memset(str_num,0,sizeof(str_num));
	switch(type) {
		case CCHARTYPE:
		case CVCHARTYPE:
		case CSTRINGTYPE:
			i=stleng(data);
			while (((data[i - 1] == '\0') || (data[i - 1] == ' ')) && (i > 0)) i--;
			data[i] = '\0';
			lua_pushstring(L, data);
			return;
		case CSHORTTYPE:
			l_data = (long) (*((short *)data));
			lua_pushinteger(L, l_data);
			return;
		case CINTTYPE:
			l_data = (long) (*((int *)data));
			lua_pushinteger(L, l_data);
			return;
		case CLONGTYPE:
		case CBIGINTTYPE:
			l_data = *((long *)data);
			lua_pushinteger(L, l_data);
			return;
		case CFLOATTYPE:
			d_data = (double) (*((float *)data));
			lua_pushnumber(L, d_data);
			return;
		case CDOUBLETYPE:
			d_data = (*((double *)data));
			lua_pushnumber(L, d_data);
			return;
		case CINT8TYPE:
			if (ifx_int8toasc((ifx_int8_t *)data, str_num, sizeof(str_num)-1) == 0) {
				l_data=atol(str_num);
				lua_pushinteger(L, l_data);
			}
			else
				lua_pushnil(L);
			return;
		case CDECIMALTYPE:
		case CMONEYTYPE:
			dectoasc((dec_t *)data, str_num, sizeof(str_num)-1, -1);
			d_data = atof(str_num);
			lua_pushnumber(L, d_data);
			return;
		case CDATETYPE:
			rfmtdate(*(int *)data, "YYYYMMDD", str_num);
			lua_pushstring(L, str_num);
			return;
		case CDTIMETYPE:
			dttoasc((dtime_t *)data, str_num);
			lua_pushstring(L, str_num);
			return;
		case CINVTYPE:
			intoasc((intrvl_t *)data, str_num);
			lua_pushstring(L, str_num);
			return;
		case CLOCATORTYPE:
			{
				ifx_loc_t *loc = (ifx_loc_t *)data;
				if (loc->loc_indicator == -1)
					lua_pushnil(L);
				else
					lua_pushlstring(L, loc->loc_buffer, loc->loc_size);
				return;
			}
		case CROWTYPE:
		case CCOLLTYPE:
		case CLVCHARTYPE:
			lua_pushlstring(L, data, len);
			return;
		case CBOOLTYPE:
			c_data = *((char *)data);
			lua_pushboolean(L, c_data);
			return;
		default:
			lua_pushnil(L);
			return;
	}
}


/*
** Push error message from sqlca 
*/
static void pusherrmsg (lua_State *L, ifx_sqlca_t *p_sqlca, char *hint) {
	if (p_sqlca->sqlcode == 0) {
		lua_pushnil(L);
	}
	else {
		lua_pushfstring(L, "%s fail, CODE:%d ISAM:%d MSG:%s",
			hint, p_sqlca->sqlcode, p_sqlca->sqlerrd[1], p_sqlca->sqlerrm);
	}
}


/*
** Get the internal database type name of the given column.
*/
static char *getcolumntype (int type) {
	switch (type) {
		case CCHARTYPE:
		case CVCHARTYPE:
		case CSTRINGTYPE:
			return "string";
		case CSHORTTYPE:
		case CINTTYPE:
		case CBIGINTTYPE:
		case CINT8TYPE:
			return "integer";
		case CFLOATTYPE:
		case CDOUBLETYPE:
		case CDECIMALTYPE:
		case CMONEYTYPE:
			return "number";
		case CDATETYPE:
			return "date";
		case CDTIMETYPE:
		case CINVTYPE:
			return "datetime";
		case CLOCATORTYPE:
		case CROWTYPE:
		case CLVCHARTYPE:
		case CFIXBINTYPE:
		case CVARBINTYPE:
			return "binary";
		case CCOLLTYPE:
			return "collection";
		case CBOOLTYPE:
			return "boolean";
		default:
			return "unknown";
	}
}


/*
** Get the internal database type length
*/
static void getcolumntypelen (int type,long len,int *out_len1,int *out_len2) {
	*out_len1 = -1;
	*out_len2 = -1;

	switch (type) {
		case CCHARTYPE:
		case CVCHARTYPE:
		case CSTRINGTYPE:
			*out_len1 = len - 1;
			return ;
		case CSHORTTYPE:
		case CINTTYPE:
		case CBIGINTTYPE:
		case CINT8TYPE:
			return ;
		case CFLOATTYPE:
			*out_len1 = 4;
			return ;
		case CDOUBLETYPE:
			*out_len1 = 8;
			return ;
		case CDECIMALTYPE:
		case CMONEYTYPE:
			*out_len1 = PRECTOT(len);
			*out_len2 = PRECDEC(len);
			return ;
		case CDATETYPE:
			return ;
		case CDTIMETYPE:
		case CINVTYPE:
			{
				*out_len1 = TU_START(len);
				*out_len2 = TU_END(len);
				return ;
			}
		case CLOCATORTYPE:
		case CROWTYPE:
		case CLVCHARTYPE:
		case CFIXBINTYPE:
		case CVARBINTYPE:
			return ;
		case CCOLLTYPE:
			return ;
		case CBOOLTYPE:
			return ;
		default:
			return ;
	}
}


/*
** Creates the lists of fields names and fields types.
*/
static void create_colinfo (lua_State *L, cur_data *cur) {
	char typename[64];
	int len_max,len_min;
	int i;
	ifx_sqlvar_t *sqlvar = NULL;

	lua_newtable(L); /* names */
	lua_newtable(L); /* types */
	for (i = 0, sqlvar = cur->cur_sqlda->sqlvar; i < cur->cur_sqlda->sqld; i++, sqlvar++) {
		lua_pushstring(L, sqlvar->sqlname);
		lua_rawseti(L, -3, i+1);
		getcolumntypelen(sqlvar->sqltype, sqlvar->sqllen, &len_max, &len_min);
		if ((len_max == -1) && (len_min == -1))
			snprintf(typename, sizeof(typename), "%.20s", getcolumntype(sqlvar->sqltype));
		else if (len_min == -1)
			snprintf(typename, sizeof(typename), "%.20s(%d)", getcolumntype(sqlvar->sqltype), len_max);
		else
			snprintf(typename, sizeof(typename), "%.20s(%d,%d)", getcolumntype(sqlvar->sqltype), len_max, len_min);
		lua_pushstring(L, typename);
		lua_rawseti(L, -2, i+1);
	}
	/* Stores the references in the cursor structure */
	cur->coltypes = luaL_ref (L, LUA_REGISTRYINDEX);
	cur->colnames = luaL_ref (L, LUA_REGISTRYINDEX);
}


/*
** Closes the cursos and nullify all structure fields.
*/
static void cur_nullify (lua_State *L, cur_data *cur) {
	/* Nullify structure fields. */
	conn_data *conn = getconnfromref(L, cur->conn);
	ifx_sqlvar_t *sqlvar = NULL;
	int i;

	if (!(conn->closed)) {
		set_conn(L, conn);
		sqli_curs_close(ESQLINTVERSION, sqli_curs_locate(ESQLINTVERSION, cur->cur_name, 768));
	}
	sqli_curs_free(ESQLINTVERSION, sqli_curs_locate(ESQLINTVERSION, cur->cur_name, 770));
	cur->closed = 1;
	free(cur->buf);
	free(cur->indicators);
	for (i = 0, sqlvar = cur->cur_sqlda->sqlvar; i < cur->cur_sqlda->sqld; i++, sqlvar++) {
		if (sqlvar->sqltype == CLOCATORTYPE) {
			ifx_loc_t *p = (ifx_loc_t *)sqlvar->sqldata;
			if (p->loc_buffer != NULL)
				free(p->loc_buffer);
		}
	}
	free(cur->cur_sqlda);
	luaL_unref(L, LUA_REGISTRYINDEX, cur->conn);
	luaL_unref(L, LUA_REGISTRYINDEX, cur->colnames);
	luaL_unref(L, LUA_REGISTRYINDEX, cur->coltypes);
}


/*
** Get another row of the given cursor.
*/
static int cur_fetch (lua_State *L) {
	cur_data *cur = getcursor(L);
	conn_data *conn = getconnfromref(L, cur->conn);
	static _FetchSpec _FS0 = { 0, 1, 0 };
	ifx_sqlvar_t *sqlvar = NULL;

	set_conn(L, conn);
	sqli_curs_fetch(ESQLINTVERSION, sqli_curs_locate(ESQLINTVERSION, cur->cur_name, 768),
		(ifx_sqlda_t *)0, cur->cur_sqlda, (char *)0, &_FS0);
	memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));
	if (sqlca.sqlcode != 0) {
		cur_nullify(L, cur);
		lua_pushnil(L);
		if (conn->conn_sqlca.sqlcode == 100) {
			return 1;
		}
		pusherrmsg(L, &(conn->conn_sqlca), "fetch cursor");
		return 2;
	}

	if (lua_istable (L, 2)) {
		const char *opts = luaL_optstring(L, 3, "n");

		if (strchr (opts, 'n') != NULL) {
			/* Copy values to numerical indices */
			int i;

			for (i = 0, sqlvar = cur->cur_sqlda->sqlvar; i < cur->cur_sqlda->sqld; i++, sqlvar++) {
				pushvalue(L, sqlvar->sqlind, sqlvar->sqltype, sqlvar->sqldata, sqlvar->sqllen);
				lua_rawseti(L, 2, i+1);
			}
		}
		if (strchr (opts, 'a') != NULL) {
			int i;

			for (i = 0, sqlvar = cur->cur_sqlda->sqlvar; i < cur->cur_sqlda->sqld; i++, sqlvar++) {
				lua_pushstring(L, sqlvar->sqlname);
				pushvalue(L, sqlvar->sqlind, sqlvar->sqltype, sqlvar->sqldata, sqlvar->sqllen);
				lua_rawset(L, 2);
			}
		}
		lua_pushvalue(L, 2);
		return 1; /* return table */
	}
	else {
		int i;
		luaL_checkstack (L, cur->cur_sqlda->sqld, LUASQL_PREFIX"too many columns");
		for (i = 0, sqlvar = cur->cur_sqlda->sqlvar; i < cur->cur_sqlda->sqld; i++, sqlvar++) {
			pushvalue(L, sqlvar->sqlind, sqlvar->sqltype, sqlvar->sqldata, sqlvar->sqllen);
		}
		return cur->cur_sqlda->sqld; /* return value number */
	}
}


/*
** The iterator of cursor
*/
static int cur_iterator (lua_State *L) {
	int i;

	lua_pop(L, 1);
	for (i = 1; !lua_isnone(L, lua_upvalueindex(i)); i++) {
		lua_pushvalue(L, lua_upvalueindex(i));
	}
	return cur_fetch(L);
}


/*
** Return the iterator of cursor
*/
static int cur_getiter (lua_State *L) {
	cur_data *cur = getcursor(L);
	const int num = lua_gettop(L) - 1;
	int i;

	for (i = 0; i < num; i++) {
		lua_pushvalue(L, i + 2);		/* push fetch parameter as upvalue */
	}
	lua_pushcclosure(L, cur_iterator, num);
	lua_pushvalue(L, 1);				/* push cursor data*/
	return 2;
}


/*
** Cursor object collector function
*/
static int cur_gc (lua_State *L) {
	cur_data *cur = (cur_data *)luaL_checkudata(L, 1, LUASQL_CURSOR_INFORMIX);
	if (cur != NULL && !(cur->closed))
		cur_nullify(L, cur);
	return 0;
}


/*
** Close the cursor on top of the stack.
** Return 1
*/
static int cur_close (lua_State *L) {
	cur_data *cur = (cur_data *)luaL_checkudata(L, 1, LUASQL_CURSOR_INFORMIX);
	luaL_argcheck(L, cur != NULL, 1, LUASQL_PREFIX"cursor expected");
	if (cur->closed) {
		lua_pushboolean(L, 0);
		return 1;
	}
	cur_nullify(L, cur);
	lua_pushboolean(L, 1);
	return 1;
}


/*
** Pushes a column information table on top of the stack.
** If the table isn't built yet, call the creator function and stores
** a reference to it on the cursor structure.
*/
static void _pushtable (lua_State *L, cur_data *cur, size_t off) {
	int *ref = (int *)((char *)cur + off);

	/* If colnames or coltypes do not exist, create both. */
	if (*ref == LUA_NOREF)
		create_colinfo(L, cur);
	
	/* Pushes the right table (colnames or coltypes) */
	lua_rawgeti (L, LUA_REGISTRYINDEX, *ref);
}
#define pushtable(L,c,m) (_pushtable(L,c,offsetof(cur_data,m)))


/*
** Return the list of field names.
*/
static int cur_getcolnames (lua_State *L) {
	pushtable(L, getcursor(L), colnames);
	return 1;
}


/*
** Return the list of field types.
*/
static int cur_getcoltypes (lua_State *L) {
	pushtable(L, getcursor(L), coltypes);
	return 1;
}


/*
** Return the field num.
*/
static int cur_getfieldnum (lua_State *L) {
	cur_data *cur = getcursor(L);
	lua_pushinteger(L, cur->cur_sqlda->sqld);
	return 1;
}


/*
** Create a new Cursor object and push it on top of the stack.
*/
static int create_cursor (lua_State *L, int conn, char *curid, ifx_sqlda_t *sqlda, char *buf, int2 *ind) {
	cur_data *cur = (cur_data *)lua_newuserdata(L, sizeof(cur_data));
	luasql_setmeta(L, LUASQL_CURSOR_INFORMIX);

	/* fill in structure */
	cur->closed = 0;
	cur->conn = LUA_NOREF;
	cur->colnames = LUA_NOREF;
	cur->coltypes = LUA_NOREF;
	strncpy(cur->cur_name,curid,sizeof(cur->cur_name));
	cur->cur_sqlda = sqlda;
	cur->buf = buf;
	cur->indicators = ind;
	lua_pushvalue (L, conn);
	cur->conn = luaL_ref(L, LUA_REGISTRYINDEX);

	return 1;
}


static int conn_gc (lua_State *L) {
	conn_data *conn=(conn_data *)luaL_checkudata(L, 1, LUASQL_CONNECTION_INFORMIX);
	if (conn != NULL && !(conn->closed)) {
		set_conn(L, conn);
		sqli_trans_rollback();
		sqli_connect_close(0, conn->conn_name, 0, 0);

		/* Nullify structure fields. */
		conn->closed = 1;
		luaL_unref(L, LUA_REGISTRYINDEX, conn->env);
	}
	return 0;
}


/*
** Close a Connection object.
*/
static int conn_close (lua_State *L) {
	conn_data *conn=(conn_data *)luaL_checkudata(L, 1, LUASQL_CONNECTION_INFORMIX);
	luaL_argcheck(L, conn != NULL, 1, LUASQL_PREFIX"connection expected");
	if (conn->closed) {
		lua_pushboolean(L, 0);
		return 1;
	}
	conn_gc(L);
	lua_pushboolean(L, 1);
	return 1;
}


/*
** Alloc buffer from description sqlda struct
*/
static int alloc_buf (ifx_sqlda_t *sqlda, char **p_buf, int2 **p_ind) {
	int i;
	int c,len = 0;
	char *buf = NULL, *p = NULL;
	ifx_sqlvar_t *sqlvar = NULL;
	int2 *ind;

	for (i = 0, sqlvar = sqlda->sqlvar; i < sqlda->sqld; i++, sqlvar++) {
		c = sqlvar->sqltype;
		toctype(sqlvar->sqltype, c);

		/* deal UDT type */
		if (ISUDTTYPE(c)) {
			switch(sqlvar->sqlxid) {
				case XID_BLOB:
				case XID_CLOB:
					sqlvar->sqltype = CLOCATORTYPE;
					sqlvar->sqllen = sizeof(ifx_loc_t);
					break;
				default:
					sqlvar->sqltype = CSTRINGTYPE;
					sqlvar->sqllen = 2048;			/* default length, data may imcomplete */
			}
		}

		/* SQLLVARCHAR convert to c style string type CSTRINGTYPE */
		if (c == SQLLVARCHAR)
			sqlvar->sqltype = CSTRINGTYPE;

		len = rtypalign(len, sqlvar->sqltype) + rtypmsize(sqlvar->sqltype, sqlvar->sqllen);
	}
	buf = (char *)malloc(len+1);
	if (buf == NULL) {
		return -1;
	}
	ind = (int2 *)calloc(sqlda->sqld,sizeof(int2));
	if (ind == NULL) {
		free(buf);
		return -1;
	}
	*p_buf = buf;
	*p_ind = ind;
	memset(buf, 0, len+1);
	for (i = 0, sqlvar = sqlda->sqlvar, p = buf; i < sqlda->sqld; i++, sqlvar++, ind++) {
		p = (char *)rtypalign((mlong)p, sqlvar->sqltype);
		sqlvar->sqldata = p;
		p += rtypmsize(sqlvar->sqltype, sqlvar->sqllen);

		/* adjust type length except datetime and decimal type */
		if ((sqlvar->sqltype != CDTIMETYPE)&&
			(sqlvar->sqltype != CINVTYPE)&&
			(sqlvar->sqltype != CDECIMALTYPE)&&
			(sqlvar->sqltype != CMONEYTYPE)) {
				sqlvar->sqllen=rtypmsize(sqlvar->sqltype, sqlvar->sqllen);
		}

		/* setup locator type */
		if (sqlvar->sqltype == CLOCATORTYPE) {
			ifx_loc_t *loc = (ifx_loc_t *)sqlvar->sqldata;
			loc->loc_loctype = LOCMEMORY;
			loc->loc_bufsize = -1;
			loc->loc_oflags = 0;
			loc->loc_mflags = LOC_ALLOC;
		}

		sqlvar->sqlind = ind;
	}

	return 0;
}


/*
** Execute an SQL statement.
** Return a Cursor object if the statement is a query, otherwise
** return the number of tuples affected by the statement.
*/
static int conn_execute (lua_State *L) {
	conn_data *conn = getconnection(L);
	size_t st_len;
	const char *statement = luaL_checklstring(L, 2, &st_len);
	char prepid[64];
	ifx_sqlda_t *sqlda=NULL;
	ifx_cursor_t *pStmt=NULL;

	set_conn(L, conn);
	conn->stmt_cnt++;
	snprintf(prepid, sizeof(prepid), "p_%lX_%d", conn, conn->stmt_cnt);
	pStmt = sqli_prep(ESQLINTVERSION, prepid, statement, (ifx_literal_t *)0, (ifx_namelist_t *)0, -1, 0, 0 );
	memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));
	if (sqlca.sqlcode != 0) {
		lua_pushnil(L);
		pusherrmsg(L, &(conn->conn_sqlca), "prepare sql");
		return 2;
	}

	sqli_describe_stmt(ESQLINTVERSION, pStmt, &sqlda, 0);
	if (sqlda->sqld == 0) {
		/* not query, execute the sql statment */
		free(sqlda);
		sqli_exec(ESQLINTVERSION, pStmt, (ifx_sqlda_t *)0, (char *)0, (struct value *)0,
			(ifx_sqlda_t *)0, (char *)0, (struct value *)0, 0);
		memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));
		if (sqlca.sqlcode != 0) {
			/* execute sql fail */
			lua_pushnil(L);
		}
		else {
			/* return affected rows */
			lua_pushinteger(L, sqlca.sqlerrd[2]);
		}
		sqli_curs_free(ESQLINTVERSION, pStmt);
		pusherrmsg(L, &(conn->conn_sqlca), "execute sql");
		return 2;
	}
	else { /* return tuples */
		char curid[64];
		char *buf = NULL;
		int2 *ind = NULL;

		snprintf(curid, sizeof(curid), "c_%lX_%d", conn, conn->stmt_cnt);

		/* alloc buf for sqlda */
		if (alloc_buf(sqlda, &buf, &ind) != 0) {
			free(sqlda);
			sqli_curs_free(ESQLINTVERSION, pStmt);
			lua_pushnil(L);
			lua_pushstring(L, "alloc fetch buffer fail");
			return 2;
		}

		/* declare cursor with hold */
		sqli_curs_decl_dynm(ESQLINTVERSION, sqli_curs_locate(ESQLINTVERSION, curid, 512), curid, pStmt, 4096, 0);
		memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));
		if (sqlca.sqlcode != 0) {
			free(buf);
			free(ind);
			free(sqlda);
			sqli_curs_free(ESQLINTVERSION, pStmt);
			lua_pushnil(L);
			pusherrmsg(L, &(conn->conn_sqlca), "declare cursor");
			return 2;
		}
		sqli_curs_free(ESQLINTVERSION, pStmt);

		/* open cursor */
		sqli_curs_open(ESQLINTVERSION, sqli_curs_locate(ESQLINTVERSION, curid, 768),
			(ifx_sqlda_t *)0, (char *)0, (struct value *)0, 0, 0);
		memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));
		if (sqlca.sqlcode != 0) {
			free(buf);
			free(ind);
			free(sqlda);
			sqli_curs_free(ESQLINTVERSION, sqli_curs_locate(ESQLINTVERSION, curid, 770));
			lua_pushnil(L);
			pusherrmsg(L, &(conn->conn_sqlca), "open cursor");
			return 2;
		}

		return create_cursor(L, 1, curid, sqlda, buf, ind);
	}
}


/*
** Commit the current transaction.
*/
static int conn_transbegin (lua_State *L) {
	conn_data *conn = getconnection (L);
	set_conn(L, conn);
	sqli_trans_begin2((mint)1);
	memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));
	if (sqlca.sqlcode != 0) {
		lua_pushboolean(L, 0);
		pusherrmsg(L, &(conn->conn_sqlca), "begin transaction");
		return 2;
	}
	conn->auto_commit = 0;
	conn->auto_begin = 0;
	lua_pushboolean(L, 1);
	return 1;
}


/*
** Commit the current transaction.
*/
static int conn_commit (lua_State *L) {
	conn_data *conn = getconnection (L);

	if (conn->auto_commit == 1) {
		lua_pushboolean(L, 1);
		return 1;
	}
	set_conn(L, conn);
	sqli_trans_commit();
	memcpy(&(conn->conn_sqlca), &sqlca, sizeof(ifx_sqlca_t));
	if (sqlca.sqlcode != 0) {
		lua_pushboolean(L, 0);
		pusherrmsg(L, &(conn->conn_sqlca), "commit transaction");
		return 2;
	}
	if (conn->auto_begin == 1) {
		sqli_trans_begin2((mint)1);
		if (sqlca.sqlcode != 0) {
			memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));
			lua_pushboolean(L, 0);
			pusherrmsg(L, &(conn->conn_sqlca), "begin transaction");
			return 2;
		}
	}
	lua_pushboolean(L, 1);
	return 1;
}


/*
** Rollback the current transaction.
*/
static int conn_rollback (lua_State *L) {
	conn_data *conn = getconnection (L);

	if (conn->auto_commit == 1) {
		lua_pushboolean(L, 0);
		lua_pushstring(L, "rollback transaction fail, auto commit mode");
		return 2;
	}
	set_conn(L, conn);
	sqli_trans_rollback();
	memcpy(&(conn->conn_sqlca), &sqlca, sizeof(ifx_sqlca_t));
	if (sqlca.sqlcode != 0) {
		lua_pushboolean(L, 0);
		pusherrmsg(L, &(conn->conn_sqlca), "rollback transaction");
		return 2;
	}
	if (conn->auto_begin == 1) {
		sqli_trans_begin2((mint)1);
		if (sqlca.sqlcode != 0) {
			memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));
			lua_pushboolean(L, 0);
			pusherrmsg(L, &(conn->conn_sqlca), "begin transaction");
			return 2;
		}
	}
	lua_pushboolean(L, 1);
	return 1;
}


/*
** Set "auto commit" property of the connection.
** If 'true', then rollback current transaction.
** If 'false', then start a new transaction.
*/
static int conn_setautocommit (lua_State *L)
{
	conn_data *conn = getconnection(L);
	set_conn(L, conn);
	if (lua_toboolean(L, 2))
	{
		/* undo active transaction - ignore errors */
		sqli_trans_rollback();
		lua_pushboolean(L, 1);
		conn->auto_commit = 1;
		conn->auto_begin = 0;
		return 1;
	}
	else
	{
		sqli_trans_begin2((mint)1);
		memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));
		if (sqlca.sqlcode != 0) {
			lua_pushboolean(L, 0);
			pusherrmsg(L, &(conn->conn_sqlca), "begin transaction");
			return 2;
		}
		else {
			conn->auto_commit = 0;
			conn->auto_begin = 1;
			lua_pushboolean(L, 1);
			return 1;
		}
	}
}


/*
** Get Last auto-increment id generated
*/
static int conn_getlastserialvalue (lua_State *L) {
	conn_data *conn = getconnection(L);
	lua_pushinteger(L, conn->conn_sqlca.sqlerrd[1]);
	return 1;
}


/*
** Get Last operation result (sqlca)
*/
static int conn_getresult (lua_State *L) {
	conn_data *conn = getconnection(L);
	const int num = lua_gettop(L);

	lua_newtable(L);
	lua_pushstring(L, "code");
	lua_pushinteger(L, conn->conn_sqlca.sqlcode);
	lua_rawset(L, -3);
	lua_pushstring(L, "isam");
	lua_pushinteger(L, conn->conn_sqlca.sqlerrd[1]);
	lua_rawset(L, -3);
	lua_pushstring(L, "rows");
	lua_pushinteger(L, conn->conn_sqlca.sqlerrd[2]);
	lua_rawset(L, -3);
	lua_pushstring(L, "errm");
	lua_pushstring(L, conn->conn_sqlca.sqlerrm);
	lua_rawset(L, -3);
	lua_pushstring(L, "err_msg");
	lua_pushfstring(L, "CODE:%d ISAM:%d MSG:%s",
		conn->conn_sqlca.sqlcode, conn->conn_sqlca.sqlerrd[1], conn->conn_sqlca.sqlerrm);
	lua_rawset(L, -3);

	if (num > 1) {
		if (!lua_isstring(L, 2)) {
			lua_pop(L, 1);
			lua_pushnil(L);
			return 1;
		}
		lua_pushvalue(L, 2);
		lua_rawget(L, -2);
		lua_remove(L, -2);
	}
	return 1;
}


/*
** Escapes a given string so that it can't break out of it's delimiting quotes
*/
static int escape_string (lua_State *L) {
	size_t len;
	const char *from = luaL_checklstring(L, 2, &len);
	char *res = malloc(len*sizeof(char)*2+1);
	char *to = res;

	if (res) {
		while(*from != '\0') {
			*(to++) = *from;
			if (*from == '\'')
				*(to++) = *from;
			from++;
		}
		*to = '\0';
		lua_pushstring(L, res);
		free(res);
		return 1;
	}
	return luasql_faildirect(L, "alloc memory fail");
}


/*
** Convert date string to internal date format
*/
static int datetoint (lua_State *L) {
	int4 dt;
	char *datestr = (char *)luaL_checkstring(L, 2);
	char *datefmt = (char *)luaL_optstring(L, 3, NULL);

	if (datefmt == NULL) {
		if (rstrdate(datestr, &dt) != 0) {
			return luasql_faildirect(L, "date convert fail");
		}
	}
	else {
		if (rdefmtdate(&dt, datefmt, datestr) != 0) {
			return luasql_faildirect(L, "date convert fail");
		}
	}
	lua_pushinteger(L, dt);
	return 1;
}


/*
** Convert internal date to date string
*/
static int inttodate (lua_State *L) {
	int4 dt = luaL_checkinteger(L, 2);
	char *datefmt = (char *)luaL_optstring(L, 3, NULL);
	char datestr[256];

	memset(datestr, 0, sizeof(datestr));
	if (datefmt == NULL) {
		if (rdatestr(dt, datestr) != 0) {
			return luasql_faildirect(L, "date convert fail");
		}
	}
	else {
		if (rfmtdate(dt, datefmt, datestr) != 0) {
			return luasql_faildirect(L, "date convert fail");
		}
	}
	lua_pushstring(L, datestr);
	return 1;
}


/*
** Create a new Connection object and push it on top of the stack.
*/
static int create_connection (lua_State *L, int env, const char *conn_id) {
	conn_data *conn = (conn_data *)lua_newuserdata(L, sizeof(conn_data));
	luasql_setmeta(L, LUASQL_CONNECTION_INFORMIX);

	/* fill in structure */
	conn->closed = 0;
	conn->env = LUA_NOREF;
	strncpy(conn->conn_name,conn_id,sizeof(conn->conn_name)-1);
	conn->stmt_cnt = 0;
	conn->auto_commit = 1;
	conn->auto_begin = 0;
	lua_pushvalue(L, env);
	conn->env = luaL_ref(L, LUA_REGISTRYINDEX);
	memcpy(&(conn->conn_sqlca),&sqlca,sizeof(ifx_sqlca_t));

	return 1;
}


/*
** Connects to a database
*/
static int env_connect (lua_State *L) {
	int r;
	env_data *env = getenvironment(L);
	const char *dbname = luaL_checkstring(L, 2);
	const char *username = luaL_optstring(L, 3, NULL);
	const char *password = luaL_optstring(L, 4, NULL);
	char connid[MAX_NAME_LENGTH];
	ifx_conn_t *_sqiconn;

	if (set_env(env) != 0) {
		return luasql_faildirect(L, "set informix server environment fail");
	}
	env->conn_cnt++;
	snprintf(connid, sizeof(connid), "C_%lX_%d", env, env->conn_cnt);
	/* Try to connect the database */
	if (username != NULL)
	{
		_sqiconn = (ifx_conn_t *)ifx_alloc_conn_user(username, password);
		sqli_connect_open(ESQLINTVERSION, 0, dbname, connid, _sqiconn, 1);
		ifx_free_conn_user(&_sqiconn);
	}
	else
	{
		sqli_connect_open(ESQLINTVERSION, 0, dbname, connid, (ifx_conn_t *)0, 1);
	}
	if (sqlca.sqlcode != 0) {
		lua_pushnil(L);
		pusherrmsg(L, &sqlca, "connect db");
		return 2;
	}
	if (restore_env(env) != 0) {
		return luasql_faildirect(L, "set informix server environment fail");
	}
	return create_connection(L, 1, connid);
}


/*
**	disconnect from server
*/
inline static void env_disconnect (env_data *env)
{
	/* disconnect all connection */
	sqli_connect_close(2, (char *)0, 0, 0);
}


/*
**
*/
static int env_gc (lua_State *L) {
	env_data *env= (env_data *)luaL_checkudata(L, 1, LUASQL_ENVIRONMENT_INFORMIX);
	if (env != NULL && !(env->closed)) {
		env_disconnect(env);
		env->closed = 1;
	}
	return 0;
}


/*
** Close environment object.
*/
static int env_close (lua_State *L) {
	env_data *env= (env_data *)luaL_checkudata(L, 1, LUASQL_ENVIRONMENT_INFORMIX);
	luaL_argcheck(L, env != NULL, 1, LUASQL_PREFIX"environment expected");
	if (env->closed) {
		lua_pushboolean(L, 0);
		return 1;
	}

	/* close connections */
	env_disconnect(env);
	env->closed = 1;
	lua_pushboolean(L, 1);
	return 1;
}


/*
** Create metatables for each class of object.
*/
static void create_metatables (lua_State *L) {
	struct luaL_Reg environment_methods[] = {
		{"__gc", env_gc},
		{"close", env_close},
		{"connect", env_connect},
		{NULL, NULL},
	};
	struct luaL_Reg connection_methods[] = {
		{"__gc", conn_gc},
		{"close", conn_close},
		{"execute", conn_execute},
		{"transbegin", conn_transbegin},
		{"commit", conn_commit},
		{"rollback", conn_rollback},
		{"setautocommit", conn_setautocommit},
		{"getlastserial", conn_getlastserialvalue},
		{"getresult", conn_getresult},
		{"escape", escape_string},
		{"datetoint", datetoint},
		{"inttodate", inttodate},
		{NULL, NULL},
	};
	struct luaL_Reg cursor_methods[] = {
		{"__gc", cur_gc},
		{"close", cur_close},
		{"getcolnames", cur_getcolnames},
		{"getcoltypes", cur_getcoltypes},
		{"getfldnum", cur_getfieldnum},
		{"fetch", cur_fetch},
		{"iterator", cur_getiter},
		{NULL, NULL},
	};
	luasql_createmeta(L, LUASQL_ENVIRONMENT_INFORMIX, environment_methods);
	luasql_createmeta(L, LUASQL_CONNECTION_INFORMIX, connection_methods);
	luasql_createmeta(L, LUASQL_CURSOR_INFORMIX, cursor_methods);
	lua_pop(L, 3);
}


/*
** Creates an Environment and returns it.
*/
static int create_environment (lua_State *L) {
	const char *env_server = luaL_optstring(L, 1, NULL);
	env_data *env = (env_data *)lua_newuserdata(L, sizeof(env_data));
	luasql_setmeta(L, LUASQL_ENVIRONMENT_INFORMIX);

	/* fill in structure */
	memset(env, 0, sizeof(env_data));
	env->old_env = getenv(ENV_INFORMIX_SVR);
	if ((env->old_env == NULL)&&(env_server == NULL)) {
		return luasql_faildirect(L, "can't found informix server environment.");
	}
	if (env->old_env != NULL) {
		env->old_env -= strlen(ENV_INFORMIX_SVR)+1;   /* poing to environment string in u area */
	}
	if (env_server != NULL) {
		snprintf(env->curr_env, sizeof(env->curr_env), "%s=%s", ENV_INFORMIX_SVR, env_server);
	}

	env->closed = 0;
	env->conn_cnt = 0;
	return 1;
}


/*
** Creates the metatables for the objects and registers the
** driver open method.
*/
LUASQL_API int luaopen_luasql_informix (lua_State *L) { 
	struct luaL_Reg driver[] = {
		{"informix", create_environment},
		{NULL, NULL},
	};
	create_metatables(L);
	lua_newtable(L);
	luaL_setfuncs(L, driver, 0);
	luasql_set_info(L);
	return 1;
}

