import { ApiError } from "../utils/ApiError.js";

/**
 * Error handler middleware
 * @param {Error} err - Error object
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next function
 */
export const errorHandler = (err, req, res, next) => {
  console.error("Error:", err);
  
  // If it's already an ApiError, use it directly
  if (err instanceof ApiError) {
    return res
      .status(err.statusCode)
      .json({
        statusCode: err.statusCode,
        message: err.message || "Something went wrong",
        success: false,
        errors: err.errors,
        data: err.data
      });
  }

  // For unhandled errors, create a generic ApiError
  const error = new ApiError(
    err.statusCode || 500,
    err.message || "Internal Server Error",
    err.errors || []
  );

  return res
    .status(error.statusCode)
    .json({
      statusCode: error.statusCode,
      message: error.message,
      success: false,
      errors: error.errors,
      data: null
    });
}; 