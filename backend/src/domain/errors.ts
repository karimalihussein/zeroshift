export class ApplicationError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly statusCode: number,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export class ConflictError extends ApplicationError {
  constructor(message: string, code = 'CONFLICT') {
    super(message, code, 409);
  }
}

export class InvalidTransitionError extends ConflictError {
  constructor(from: string, to: string) {
    super(`Invalid migration transition: ${from} → ${to}`, 'INVALID_TRANSITION');
  }
}

export class NotFoundError extends ApplicationError {
  constructor(message: string, code = 'NOT_FOUND') {
    super(message, code, 404);
  }
}

export class ValidationError extends ApplicationError {
  constructor(message: string, code = 'VALIDATION_ERROR') {
    super(message, code, 422);
  }
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
