#![allow(unsafe_code)] // ffi calls
extern crate libsqlite3_sys as ffi;

use std::ffi::{c_char, c_int, c_void, CStr, CString, NulError};
use std::io::{stderr, Write};
use std::os::raw as libc;
use std::ptr::NonNull;
use std::{mem, ptr, slice, str};

use super::functions::{build_sql_function_args, process_sql_function_result};
use super::serialized_database::SerializedDatabase;
use super::stmt::ensure_sqlite_ok;
use super::{Sqlite, SqliteAggregateFunction, SqliteTokenizerFunction};
use crate::deserialize::FromSqlRow;
use crate::result::Error::DatabaseError;
use crate::result::*;
use crate::serialize::ToSql;
use crate::sql_types::HasSqlType;

/// For use in FFI function, which cannot unwind.
/// Print the message, ask to open an issue at Github and [`abort`](std::process::abort).
macro_rules! assert_fail {
    ($fmt:expr $(,$args:tt)*) => {
        eprint!(concat!(
            $fmt,
            "If you see this message, please open an issue at https://github.com/diesel-rs/diesel/issues/new.\n",
            "Source location: {}:{}\n",
        ), $($args,)* file!(), line!());
        std::process::abort()
    };
}

#[allow(missing_debug_implementations, missing_copy_implementations)]
pub(super) struct RawConnection {
    pub(super) internal_connection: NonNull<ffi::sqlite3>,
}

impl RawConnection {
    pub(super) fn establish(database_url: &str) -> ConnectionResult<Self> {
        let mut conn_pointer = ptr::null_mut();

        let database_url = if database_url.starts_with("sqlite://") {
            CString::new(database_url.replacen("sqlite://", "file:", 1))?
        } else {
            CString::new(database_url)?
        };
        let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_URI;
        let connection_status = unsafe {
            ffi::sqlite3_open_v2(database_url.as_ptr(), &mut conn_pointer, flags, ptr::null())
        };

        match connection_status {
            ffi::SQLITE_OK => {
                let conn_pointer = unsafe { NonNull::new_unchecked(conn_pointer) };
                Ok(RawConnection {
                    internal_connection: conn_pointer,
                })
            }
            err_code => {
                let message = super::error_message(err_code);
                Err(ConnectionError::BadConnection(message.into()))
            }
        }
    }

    pub(super) fn exec(&self, query: &str) -> QueryResult<()> {
        let query = CString::new(query)?;
        let callback_fn = None;
        let callback_arg = ptr::null_mut();
        let result = unsafe {
            ffi::sqlite3_exec(
                self.internal_connection.as_ptr(),
                query.as_ptr(),
                callback_fn,
                callback_arg,
                ptr::null_mut(),
            )
        };

        ensure_sqlite_ok(result, self.internal_connection.as_ptr())
    }

    pub(super) fn rows_affected_by_last_query(&self) -> usize {
        unsafe { ffi::sqlite3_changes(self.internal_connection.as_ptr()) as usize }
    }

    pub(super) fn register_sql_function<F, Ret, RetSqlType>(
        &self,
        fn_name: &str,
        num_args: usize,
        deterministic: bool,
        f: F,
    ) -> QueryResult<()>
    where
        F: FnMut(&Self, &mut [*mut ffi::sqlite3_value]) -> QueryResult<Ret>
            + std::panic::UnwindSafe
            + Send
            + 'static,
        Ret: ToSql<RetSqlType, Sqlite>,
        Sqlite: HasSqlType<RetSqlType>,
    {
        let callback_fn = Box::into_raw(Box::new(CustomFunctionUserPtr {
            callback: f,
            function_name: fn_name.to_owned(),
        }));
        let fn_name = Self::get_fn_name(fn_name)?;
        let flags = Self::get_flags(deterministic);

        let result = unsafe {
            ffi::sqlite3_create_function_v2(
                self.internal_connection.as_ptr(),
                fn_name.as_ptr(),
                num_args as _,
                flags,
                callback_fn as *mut _,
                Some(run_custom_function::<F, Ret, RetSqlType>),
                None,
                None,
                Some(destroy_boxed::<CustomFunctionUserPtr<F>>),
            )
        };

        Self::process_sql_function_result(result)
    }

    pub(super) fn register_aggregate_function<ArgsSqlType, RetSqlType, Args, Ret, A>(
        &self,
        fn_name: &str,
        num_args: usize,
    ) -> QueryResult<()>
    where
        A: SqliteAggregateFunction<Args, Output = Ret> + 'static + Send + std::panic::UnwindSafe,
        Args: FromSqlRow<ArgsSqlType, Sqlite>,
        Ret: ToSql<RetSqlType, Sqlite>,
        Sqlite: HasSqlType<RetSqlType>,
    {
        let fn_name = Self::get_fn_name(fn_name)?;
        let flags = Self::get_flags(false);

        let result = unsafe {
            ffi::sqlite3_create_function_v2(
                self.internal_connection.as_ptr(),
                fn_name.as_ptr(),
                num_args as _,
                flags,
                ptr::null_mut(),
                None,
                Some(run_aggregator_step_function::<_, _, _, _, A>),
                Some(run_aggregator_final_function::<_, _, _, _, A>),
                None,
            )
        };

        Self::process_sql_function_result(result)
    }

    pub(super) fn register_collation_function<F>(
        &self,
        collation_name: &str,
        collation: F,
    ) -> QueryResult<()>
    where
        F: Fn(&str, &str) -> std::cmp::Ordering + std::panic::UnwindSafe + Send + 'static,
    {
        let callback_fn = Box::into_raw(Box::new(CollationUserPtr {
            callback: collation,
            collation_name: collation_name.to_owned(),
        }));
        let collation_name = Self::get_fn_name(collation_name)?;

        let result = unsafe {
            ffi::sqlite3_create_collation_v2(
                self.internal_connection.as_ptr(),
                collation_name.as_ptr(),
                ffi::SQLITE_UTF8,
                callback_fn as *mut _,
                Some(run_collation_function::<F>),
                Some(destroy_boxed::<CollationUserPtr<F>>),
            )
        };

        let result = Self::process_sql_function_result(result);
        if result.is_err() {
            destroy_boxed::<CollationUserPtr<F>>(callback_fn as *mut _);
        }
        result
    }

    pub(super) fn serialize(&mut self) -> SerializedDatabase {
        unsafe {
            let mut size: ffi::sqlite3_int64 = 0;
            let data_ptr = ffi::sqlite3_serialize(
                self.internal_connection.as_ptr(),
                std::ptr::null(),
                &mut size as *mut _,
                0,
            );
            SerializedDatabase::new(data_ptr, size as usize)
        }
    }

    pub(super) fn deserialize(&mut self, data: &[u8]) -> QueryResult<()> {
        // the cast for `ffi::SQLITE_DESERIALIZE_READONLY` is required for old libsqlite3-sys versions
        #[allow(clippy::unnecessary_cast)]
        unsafe {
            let result = ffi::sqlite3_deserialize(
                self.internal_connection.as_ptr(),
                std::ptr::null(),
                data.as_ptr() as *mut u8,
                data.len() as i64,
                data.len() as i64,
                ffi::SQLITE_DESERIALIZE_READONLY as u32,
            );

            ensure_sqlite_ok(result, self.internal_connection.as_ptr())
        }
    }

    pub(super) fn register_custom_tokenizer<T: SqliteTokenizerFunction>(
        &self,
        data: T::Data,
        name: &str,
    ) -> QueryResult<()> {
        let mut stmt = ptr::null_mut();
        let mut unused_portion = ptr::null();
        // the cast for `ffi::SQLITE_PREPARE_PERSISTENT` is required for old libsqlite3-sys versions
        let select_fts = "SELECT fts5(?1)";
        #[allow(clippy::unnecessary_cast)]
        let prepare_result = unsafe {
            ffi::sqlite3_prepare_v3(
                self.internal_connection.as_ptr(),
                CString::new(select_fts)?.as_ptr(),
                select_fts.len() as libc::c_int,
                0,
                &mut stmt,
                &mut unused_portion,
            )
        };
        ensure_sqlite_ok(prepare_result, self.internal_connection.as_ptr())?;

        let mut p_api: *mut ffi::fts5_api = ptr::null_mut();
        let bind_result = unsafe {
            ffi::sqlite3_bind_pointer(
                stmt,
                1,
                (&mut p_api) as *mut _ as *mut libc::c_void,
                "fts5_api_ptr\0".as_ptr().cast::<libc::c_char>(),
                None,
            )
        };
        ensure_sqlite_ok(bind_result, self.internal_connection.as_ptr())?;

        unsafe {
            ffi::sqlite3_step(stmt);
            ffi::sqlite3_finalize(stmt);
        }
        if p_api.is_null() {
            return Err(DatabaseError(
                DatabaseErrorKind::FtsApiFailure,
                Box::new("fts5_api_ptr is null".to_string()),
            ));
        }

        let tokenizer_name = Self::get_fn_name(name)?;
        let data = Box::into_raw(Box::new(data));

        unsafe {
            (*p_api).xCreateTokenizer.map(|f| {
                f(
                    p_api,
                    tokenizer_name.as_ptr(),
                    data.cast::<libc::c_void>(),
                    &mut ffi::fts5_tokenizer {
                        xCreate: Some(run_create_tokenizer::<T>),
                        xDelete: Some(run_delete_tokenizer::<T>),
                        xTokenize: Some(run_tokenizer::<T>),
                    },
                    Some(destroy_boxed::<T::Data>),
                )
            });
        }

        Ok(())
    }

    fn get_fn_name(fn_name: &str) -> Result<CString, NulError> {
        CString::new(fn_name)
    }

    fn get_flags(deterministic: bool) -> i32 {
        let mut flags = ffi::SQLITE_UTF8;
        if deterministic {
            flags |= ffi::SQLITE_DETERMINISTIC;
        }
        flags
    }

    fn process_sql_function_result(result: i32) -> Result<(), Error> {
        if result == ffi::SQLITE_OK {
            Ok(())
        } else {
            let error_message = super::error_message(result);
            Err(DatabaseError(
                DatabaseErrorKind::Unknown,
                Box::new(error_message.to_string()),
            ))
        }
    }
}

impl Drop for RawConnection {
    fn drop(&mut self) {
        use std::thread::panicking;

        let close_result = unsafe { ffi::sqlite3_close(self.internal_connection.as_ptr()) };
        if close_result != ffi::SQLITE_OK {
            let error_message = super::error_message(close_result);
            if panicking() {
                write!(stderr(), "Error closing SQLite connection: {error_message}")
                    .expect("Error writing to `stderr`");
            } else {
                panic!("Error closing SQLite connection: {}", error_message);
            }
        }
    }
}

enum SqliteCallbackError {
    Abort(&'static str),
    DieselError(crate::result::Error),
    Panic(String),
}

impl SqliteCallbackError {
    fn emit(&self, ctx: *mut ffi::sqlite3_context) {
        let s;
        let msg = match self {
            SqliteCallbackError::Abort(msg) => *msg,
            SqliteCallbackError::DieselError(e) => {
                s = e.to_string();
                &s
            }
            SqliteCallbackError::Panic(msg) => msg,
        };
        unsafe {
            context_error_str(ctx, msg);
        }
    }
}

impl From<crate::result::Error> for SqliteCallbackError {
    fn from(e: crate::result::Error) -> Self {
        Self::DieselError(e)
    }
}

struct CustomFunctionUserPtr<F> {
    callback: F,
    function_name: String,
}

#[allow(warnings)]
extern "C" fn run_custom_function<F, Ret, RetSqlType>(
    ctx: *mut ffi::sqlite3_context,
    num_args: libc::c_int,
    value_ptr: *mut *mut ffi::sqlite3_value,
) where
    F: FnMut(&RawConnection, &mut [*mut ffi::sqlite3_value]) -> QueryResult<Ret>
        + std::panic::UnwindSafe
        + Send
        + 'static,
    Ret: ToSql<RetSqlType, Sqlite>,
    Sqlite: HasSqlType<RetSqlType>,
{
    use std::ops::Deref;
    static NULL_DATA_ERR: &str = "An unknown error occurred. sqlite3_user_data returned a null pointer. This should never happen.";
    static NULL_CONN_ERR: &str = "An unknown error occurred. sqlite3_context_db_handle returned a null pointer. This should never happen.";

    let conn = match unsafe { NonNull::new(ffi::sqlite3_context_db_handle(ctx)) } {
        // We use `ManuallyDrop` here because we do not want to run the
        // Drop impl of `RawConnection` as this would close the connection
        Some(conn) => mem::ManuallyDrop::new(RawConnection {
            internal_connection: conn,
        }),
        None => {
            unsafe { context_error_str(ctx, NULL_CONN_ERR) };
            return;
        }
    };

    let data_ptr = unsafe { ffi::sqlite3_user_data(ctx) };

    let mut data_ptr = match NonNull::new(data_ptr as *mut CustomFunctionUserPtr<F>) {
        None => unsafe {
            context_error_str(ctx, NULL_DATA_ERR);
            return;
        },
        Some(mut f) => f,
    };
    let data_ptr = unsafe { data_ptr.as_mut() };

    // We need this to move the reference into the catch_unwind part
    // this is sound as `F` itself and the stored string is `UnwindSafe`
    let callback = std::panic::AssertUnwindSafe(&mut data_ptr.callback);

    let result = std::panic::catch_unwind(move || {
        let _ = &callback;
        let args = unsafe { slice::from_raw_parts_mut(value_ptr, num_args as _) };
        let res = (callback.0)(&*conn, args)?;
        let value = process_sql_function_result(&res)?;
        // We've checked already that ctx is not null
        unsafe {
            value.result_of(&mut *ctx);
        }
        Ok(())
    })
    .unwrap_or_else(|p| Err(SqliteCallbackError::Panic(data_ptr.function_name.clone())));
    if let Err(e) = result {
        e.emit(ctx);
    }
}

// Need a custom option type here, because the std lib one does not have guarantees about the discriminate values
// See: https://github.com/rust-lang/rfcs/blob/master/text/2195-really-tagged-unions.md#opaque-tags
#[repr(u8)]
enum OptionalAggregator<A> {
    // Discriminant is 0
    None,
    Some(A),
}

#[allow(warnings)]
extern "C" fn run_aggregator_step_function<ArgsSqlType, RetSqlType, Args, Ret, A>(
    ctx: *mut ffi::sqlite3_context,
    num_args: libc::c_int,
    value_ptr: *mut *mut ffi::sqlite3_value,
) where
    A: SqliteAggregateFunction<Args, Output = Ret> + 'static + Send + std::panic::UnwindSafe,
    Args: FromSqlRow<ArgsSqlType, Sqlite>,
    Ret: ToSql<RetSqlType, Sqlite>,
    Sqlite: HasSqlType<RetSqlType>,
{
    let result = std::panic::catch_unwind(move || {
        let args = unsafe { slice::from_raw_parts_mut(value_ptr, num_args as _) };
        run_aggregator_step::<A, Args, ArgsSqlType>(ctx, args)
    })
    .unwrap_or_else(|e| {
        Err(SqliteCallbackError::Panic(format!(
            "{}::step() panicked",
            std::any::type_name::<A>()
        )))
    });

    match result {
        Ok(()) => {}
        Err(e) => e.emit(ctx),
    }
}

fn run_aggregator_step<A, Args, ArgsSqlType>(
    ctx: *mut ffi::sqlite3_context,
    args: &mut [*mut ffi::sqlite3_value],
) -> Result<(), SqliteCallbackError>
where
    A: SqliteAggregateFunction<Args>,
    Args: FromSqlRow<ArgsSqlType, Sqlite>,
{
    static NULL_AG_CTX_ERR: &str = "An unknown error occurred. sqlite3_aggregate_context returned a null pointer. This should never happen.";
    static NULL_CTX_ERR: &str =
        "We've written the aggregator to the aggregate context, but it could not be retrieved.";

    let aggregate_context = unsafe {
        // This block of unsafe code makes the following assumptions:
        //
        // * sqlite3_aggregate_context allocates sizeof::<OptionalAggregator<A>>
        //   bytes of zeroed memory as documented here:
        //   https://www.sqlite.org/c3ref/aggregate_context.html
        //   A null pointer is returned for negative or zero sized types,
        //   which should be impossible in theory. We check that nevertheless
        //
        // * OptionalAggregator::None has a discriminant of 0 as specified by
        //   #[repr(u8)] + RFC 2195
        //
        // * If all bytes are zero, the discriminant is also zero, so we can
        //   assume that we get OptionalAggregator::None in this case. This is
        //   not UB as we only access the discriminant here, so we do not try
        //   to read any other zeroed memory. After that we initialize our enum
        //   by writing a correct value at this location via ptr::write_unaligned
        //
        // * We use ptr::write_unaligned as we did not found any guarantees that
        //   the memory will have a correct alignment.
        //   (Note I(weiznich): would assume that it is aligned correctly, but we
        //    we cannot guarantee it, so better be safe than sorry)
        ffi::sqlite3_aggregate_context(ctx, std::mem::size_of::<OptionalAggregator<A>>() as i32)
    };
    let aggregate_context = NonNull::new(aggregate_context as *mut OptionalAggregator<A>);
    let aggregator = unsafe {
        match aggregate_context.map(|a| &mut *a.as_ptr()) {
            Some(&mut OptionalAggregator::Some(ref mut agg)) => agg,
            Some(a_ptr @ &mut OptionalAggregator::None) => {
                ptr::write_unaligned(a_ptr as *mut _, OptionalAggregator::Some(A::default()));
                if let OptionalAggregator::Some(ref mut agg) = a_ptr {
                    agg
                } else {
                    return Err(SqliteCallbackError::Abort(NULL_CTX_ERR));
                }
            }
            None => {
                return Err(SqliteCallbackError::Abort(NULL_AG_CTX_ERR));
            }
        }
    };
    let args = build_sql_function_args::<ArgsSqlType, Args>(args)?;

    aggregator.step(args);
    Ok(())
}

extern "C" fn run_aggregator_final_function<ArgsSqlType, RetSqlType, Args, Ret, A>(
    ctx: *mut ffi::sqlite3_context,
) where
    A: SqliteAggregateFunction<Args, Output = Ret> + 'static + Send,
    Args: FromSqlRow<ArgsSqlType, Sqlite>,
    Ret: ToSql<RetSqlType, Sqlite>,
    Sqlite: HasSqlType<RetSqlType>,
{
    static NO_AGGREGATOR_FOUND: &str = "We've written to the aggregator in the xStep callback. If xStep was never called, then ffi::sqlite_aggregate_context() would have returned a NULL pointer.";
    let aggregate_context = unsafe {
        // Within the xFinal callback, it is customary to set nBytes to 0 so no pointless memory
        // allocations occur, a null pointer is returned in this case
        // See: https://www.sqlite.org/c3ref/aggregate_context.html
        //
        // For the reasoning about the safety of the OptionalAggregator handling
        // see the comment in run_aggregator_step_function.
        ffi::sqlite3_aggregate_context(ctx, 0)
    };

    let result = std::panic::catch_unwind(|| {
        let mut aggregate_context = NonNull::new(aggregate_context as *mut OptionalAggregator<A>);

        let aggregator = if let Some(a) = aggregate_context.as_mut() {
            let a = unsafe { a.as_mut() };
            match std::mem::replace(a, OptionalAggregator::None) {
                OptionalAggregator::None => {
                    return Err(SqliteCallbackError::Abort(NO_AGGREGATOR_FOUND));
                }
                OptionalAggregator::Some(a) => Some(a),
            }
        } else {
            None
        };

        let res = A::finalize(aggregator);
        let value = process_sql_function_result(&res)?;
        // We've checked already that ctx is not null
        unsafe {
            value.result_of(&mut *ctx);
        }
        Ok(())
    })
    .unwrap_or_else(|_e| {
        Err(SqliteCallbackError::Panic(format!(
            "{}::finalize() panicked",
            std::any::type_name::<A>()
        )))
    });
    if let Err(e) = result {
        e.emit(ctx);
    }
}

unsafe fn context_error_str(ctx: *mut ffi::sqlite3_context, error: &str) {
    ffi::sqlite3_result_error(ctx, error.as_ptr() as *const _, error.len() as _);
}

struct CollationUserPtr<F> {
    callback: F,
    collation_name: String,
}

#[allow(warnings)]
extern "C" fn run_collation_function<F>(
    user_ptr: *mut libc::c_void,
    lhs_len: libc::c_int,
    lhs_ptr: *const libc::c_void,
    rhs_len: libc::c_int,
    rhs_ptr: *const libc::c_void,
) -> libc::c_int
where
    F: Fn(&str, &str) -> std::cmp::Ordering + Send + std::panic::UnwindSafe + 'static,
{
    let user_ptr = user_ptr as *const CollationUserPtr<F>;
    let user_ptr = std::panic::AssertUnwindSafe(unsafe { user_ptr.as_ref() });

    let result = std::panic::catch_unwind(|| {
        let user_ptr = user_ptr.ok_or_else(|| {
            SqliteCallbackError::Abort(
                "Got a null pointer as data pointer. This should never happen",
            )
        })?;
        for (ptr, len, side) in &[(rhs_ptr, rhs_len, "rhs"), (lhs_ptr, lhs_len, "lhs")] {
            if *len < 0 {
                assert_fail!(
                    "An unknown error occurred. {}_len is negative. This should never happen.",
                    side
                );
            }
            if ptr.is_null() {
                assert_fail!(
                "An unknown error occurred. {}_ptr is a null pointer. This should never happen.",
                side
            );
            }
        }

        let (rhs, lhs) = unsafe {
            // Depending on the eTextRep-parameter to sqlite3_create_collation_v2() the strings can
            // have various encodings. register_collation_function() always selects SQLITE_UTF8, so the
            // pointers point to valid UTF-8 strings (assuming correct behavior of libsqlite3).
            (
                str::from_utf8(slice::from_raw_parts(rhs_ptr as *const u8, rhs_len as _)),
                str::from_utf8(slice::from_raw_parts(lhs_ptr as *const u8, lhs_len as _)),
            )
        };

        let rhs =
            rhs.map_err(|_| SqliteCallbackError::Abort("Got an invalid UTF-8 string for rhs"))?;
        let lhs =
            lhs.map_err(|_| SqliteCallbackError::Abort("Got an invalid UTF-8 string for lhs"))?;

        Ok((user_ptr.callback)(rhs, lhs))
    })
    .unwrap_or_else(|p| {
        Err(SqliteCallbackError::Panic(
            user_ptr
                .map(|u| u.collation_name.clone())
                .unwrap_or_default(),
        ))
    });

    match result {
        Ok(std::cmp::Ordering::Less) => -1,
        Ok(std::cmp::Ordering::Equal) => 0,
        Ok(std::cmp::Ordering::Greater) => 1,
        Err(SqliteCallbackError::Abort(a)) => {
            eprintln!(
                "Collation function {} failed with: {}",
                user_ptr
                    .map(|c| &c.collation_name as &str)
                    .unwrap_or_default(),
                a
            );
            std::process::abort()
        }
        Err(SqliteCallbackError::DieselError(e)) => {
            eprintln!(
                "Collation function {} failed with: {}",
                user_ptr
                    .map(|c| &c.collation_name as &str)
                    .unwrap_or_default(),
                e
            );
            std::process::abort()
        }
        Err(SqliteCallbackError::Panic(msg)) => {
            eprintln!("Collation function {} panicked", msg);
            std::process::abort()
        }
    }
}

#[allow(warnings)]
unsafe extern "C" fn run_create_tokenizer<T: SqliteTokenizerFunction>(
    data: *mut c_void,
    args: *mut *const c_char,
    nargs: c_int,
    tokenizer: *mut *mut ffi::Fts5Tokenizer,
) -> c_int {
    let user_ptr = data as *const T::Data;
    let user_ptr = std::panic::AssertUnwindSafe(unsafe { user_ptr.as_ref() });

    let result = std::panic::catch_unwind(|| {
        let user_ptr = user_ptr.ok_or_else(|| {
            SqliteCallbackError::Abort(
                "Got a null pointer as data pointer. This should never happen",
            )
        })?;

        let args = (0..nargs as usize)
            .map(|i| *args.add(i))
            .map(|s| CStr::from_ptr(s).to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        Ok(T::new(user_ptr, args)?)
    })
    .unwrap_or_else(|p| {
        Err(SqliteCallbackError::Panic(
            "Custom tokenizer create function failed.".to_string(),
        ))
    });

    match result {
        Ok(t) => {
            *tokenizer = Box::into_raw(Box::new(t)).cast::<ffi::Fts5Tokenizer>();
            ffi::SQLITE_OK
        }
        Err(SqliteCallbackError::Abort(a)) => {
            eprintln!("Custom tokenizer create function failed with: {}", a);
            std::process::abort()
        }
        Err(SqliteCallbackError::DieselError(e)) => {
            eprintln!("Custom tokenizer create function failed with: {}", e);
            std::process::abort()
        }
        Err(SqliteCallbackError::Panic(msg)) => {
            eprintln!("Collation function {} panicked", msg);
            std::process::abort()
        }
    }
}

#[allow(warnings)]
extern "C" fn run_delete_tokenizer<T: SqliteTokenizerFunction>(v: *mut ffi::Fts5Tokenizer) {
    let ptr = v as *mut T;
    unsafe { std::mem::drop(Box::from_raw(ptr)) };
}

#[allow(warnings)]
unsafe extern "C" fn run_tokenizer<T: SqliteTokenizerFunction>(
    this: *mut ffi::Fts5Tokenizer,
    ctx: *mut c_void,
    flags: c_int,
    data: *const c_char,
    data_len: c_int,
    push_token: Option<
        unsafe extern "C" fn(*mut c_void, c_int, *const c_char, c_int, c_int, c_int) -> c_int,
    >,
) -> c_int {
    let this = &mut *this.cast::<T>();
    let data = std::slice::from_raw_parts(data.cast::<u8>(), data_len as usize);

    let push_token = push_token.unwrap();
    let push_token =
        |token: &[u8], range: std::ops::Range<usize>, colocated: bool| -> QueryResult<()> {
            let ntoken = c_int::try_from(token.len()).expect("Token length is took long");
            assert!(
                range.start <= data.len() && range.end <= data.len(),
                "Token range is invalid. Range is {:?}, data length is {}",
                range,
                data.len(),
            );
            let start = range.start as c_int;
            let end = range.end as c_int;
            let flags = if colocated {
                ffi::FTS5_TOKEN_COLOCATED
            } else {
                0
            };

            let res = (push_token)(
                ctx,
                flags,
                token.as_ptr().cast::<c_char>(),
                ntoken,
                start,
                end,
            );
            if res == ffi::SQLITE_OK {
                Ok(())
            } else {
                todo!()
                // Err(rusqlite::Error::SqliteFailure(
                // 	rusqlite::ffi::Error::new(res),
                // 	None,
                // ))
            }
        };

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        this.tokenize(flags, data, push_token)
    })) {
        Ok(Ok(())) => ffi::SQLITE_OK,
        // Ok(Err(rusqlite::Error::SqliteFailure(e, _))) => e.extended_code, TODO
        Ok(Err(_)) => ffi::SQLITE_ERROR,
        Err(msg) => {
            // log::error!(
            //     "<{} as Tokenizer>::tokenize paniced: {}",
            //     std::any::type_name::<T>(),
            //     panic_err_to_str(&msg)
            // );
            ffi::SQLITE_ERROR
        }
    }
}

extern "C" fn destroy_boxed<F>(data: *mut libc::c_void) {
    let ptr = data as *mut F;
    unsafe { std::mem::drop(Box::from_raw(ptr)) };
}
