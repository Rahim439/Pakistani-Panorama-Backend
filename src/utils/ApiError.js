class ApiError extends Error {
    constructor(statusCode, message = "Something went wrong!", errors = []) {
      super(message);
      this.data = null;
      this.statusCode = statusCode;
      this.success = false;
      this.message = message;
      this.errors = errors;
    }
  }
  
  export { ApiError };
  