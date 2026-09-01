using System.Net;
using System.Text.Json;
using BuildingBlocks.Exceptions;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace PosCafe.ServiceDefaults;

public sealed class ExceptionHandlingMiddleware(
    RequestDelegate next,
    ILogger<ExceptionHandlingMiddleware> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await next(context);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Unhandled exception for {Method} {Path}", context.Request.Method, context.Request.Path);
            await WriteProblemDetailsAsync(context, exception);
        }
    }

    private static async Task WriteProblemDetailsAsync(HttpContext context, Exception exception)
    {
        var (status, title) = exception switch
        {
            ValidationException => ((int)HttpStatusCode.BadRequest, "Validation failed"),
            UnauthorizedException => ((int)HttpStatusCode.Unauthorized, "Unauthorized"),
            ForbiddenException => ((int)HttpStatusCode.Forbidden, "Forbidden"),
            NotFoundException => ((int)HttpStatusCode.NotFound, "Resource not found"),
            ConflictException => ((int)HttpStatusCode.Conflict, "Conflict"),
            DomainException => ((int)HttpStatusCode.UnprocessableEntity, "Business rule violated"),
            _ => ((int)HttpStatusCode.InternalServerError, "An unexpected error occurred")
        };

        var problem = new
        {
            type = $"https://httpstatuses.com/{status}",
            title,
            status,
            detail = exception is DomainException ? exception.Message : "Please contact support with the correlation ID.",
            code = exception is DomainException domainException ? domainException.Code : "internal_error",
            traceId = context.TraceIdentifier,
            errors = exception is ValidationException validationException ? validationException.Errors : null
        };

        context.Response.StatusCode = status;
        context.Response.ContentType = "application/problem+json";
        await context.Response.WriteAsync(JsonSerializer.Serialize(problem, JsonOptions));
    }
}
