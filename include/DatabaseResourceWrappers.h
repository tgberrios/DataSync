#ifndef DATABASE_RESOURCE_WRAPPERS_H
#define DATABASE_RESOURCE_WRAPPERS_H

#include <memory>
#include <mysql/mysql.h>
#include <optional>
#include <sql.h>
#include <sqlext.h>

// RAII wrapper for MySQL connections
class MySQLConnection {
private:
  std::unique_ptr<MYSQL, decltype(&mysql_close)> conn_;

public:
  MySQLConnection() : conn_(mysql_init(nullptr), mysql_close) {}

  explicit MySQLConnection(MYSQL *conn) : conn_(conn, mysql_close) {}

  // Move constructor
  MySQLConnection(MySQLConnection &&other) noexcept
      : conn_(std::move(other.conn_)) {}

  // Move assignment
  MySQLConnection &operator=(MySQLConnection &&other) noexcept {
    if (this != &other) {
      conn_ = std::move(other.conn_);
    }
    return *this;
  }

  // Delete copy constructor and assignment
  MySQLConnection(const MySQLConnection &) = delete;
  MySQLConnection &operator=(const MySQLConnection &) = delete;

  MYSQL *get() const noexcept { return conn_.get(); }
  MYSQL *release() noexcept { return conn_.release(); }

  bool is_valid() const noexcept {
    return conn_ != nullptr && conn_.get() != nullptr;
  }

  operator bool() const noexcept { return is_valid(); }
};

// RAII wrapper for ODBC Environment handles
class ODBCEnvironment {
private:
  std::unique_ptr<void, void (*)(void *)> handle_;

  static void freeEnvHandle(void *handle) {
    if (handle) {
      SQLFreeHandle(SQL_HANDLE_ENV, handle);
    }
  }

public:
  ODBCEnvironment() : handle_(nullptr, freeEnvHandle) {
    void *handle = nullptr;
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &handle) ==
        SQL_SUCCESS) {
      handle_.reset(handle);
    }
  }

  // Move constructor
  ODBCEnvironment(ODBCEnvironment &&other) noexcept
      : handle_(std::move(other.handle_)) {}

  // Move assignment
  ODBCEnvironment &operator=(ODBCEnvironment &&other) noexcept {
    if (this != &other) {
      handle_ = std::move(other.handle_);
    }
    return *this;
  }

  // Delete copy constructor and assignment
  ODBCEnvironment(const ODBCEnvironment &) = delete;
  ODBCEnvironment &operator=(const ODBCEnvironment &) = delete;

  SQLHENV get() const noexcept { return static_cast<SQLHENV>(handle_.get()); }
  void *release() noexcept { return handle_.release(); }

  bool is_valid() const noexcept {
    return handle_ != nullptr && handle_.get() != nullptr;
  }

  operator bool() const noexcept { return is_valid(); }
};

// RAII wrapper for ODBC Connection handles
class ODBCConnection {
private:
  std::unique_ptr<void, void (*)(void *)> handle_;

  static void freeConnHandle(void *handle) {
    if (handle) {
      SQLFreeHandle(SQL_HANDLE_DBC, handle);
    }
  }

public:
  explicit ODBCConnection(SQLHENV env) : handle_(nullptr, freeConnHandle) {
    void *handle = nullptr;
    if (SQLAllocHandle(SQL_HANDLE_DBC, env, &handle) == SQL_SUCCESS) {
      handle_.reset(handle);
    }
  }

  // Move constructor
  ODBCConnection(ODBCConnection &&other) noexcept
      : handle_(std::move(other.handle_)) {}

  // Move assignment
  ODBCConnection &operator=(ODBCConnection &&other) noexcept {
    if (this != &other) {
      handle_ = std::move(other.handle_);
    }
    return *this;
  }

  // Delete copy constructor and assignment
  ODBCConnection(const ODBCConnection &) = delete;
  ODBCConnection &operator=(const ODBCConnection &) = delete;

  SQLHDBC get() const noexcept { return static_cast<SQLHDBC>(handle_.get()); }
  void *release() noexcept { return handle_.release(); }

  bool is_valid() const noexcept {
    return handle_ != nullptr && handle_.get() != nullptr;
  }

  operator bool() const noexcept { return is_valid(); }
};

// RAII wrapper for MySQL results
class MySQLResult {
private:
  std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> result_;

public:
  MySQLResult() : result_(nullptr, mysql_free_result) {}

  explicit MySQLResult(MYSQL_RES *result)
      : result_(result, mysql_free_result) {}

  // Move constructor
  MySQLResult(MySQLResult &&other) noexcept
      : result_(std::move(other.result_)) {}

  // Move assignment
  MySQLResult &operator=(MySQLResult &&other) noexcept {
    if (this != &other) {
      result_ = std::move(other.result_);
    }
    return *this;
  }

  // Delete copy constructor and assignment
  MySQLResult(const MySQLResult &) = delete;
  MySQLResult &operator=(const MySQLResult &) = delete;

  MYSQL_RES *get() const noexcept { return result_.get(); }
  MYSQL_RES *release() noexcept { return result_.release(); }

  bool is_valid() const noexcept {
    return result_ != nullptr && result_.get() != nullptr;
  }

  operator bool() const noexcept { return is_valid(); }
};

#endif // DATABASE_RESOURCE_WRAPPERS_H
