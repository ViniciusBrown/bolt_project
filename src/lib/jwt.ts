import * as jose from 'jose';

// Secret key for JWT signing (in a real app, this would be an environment variable)
const JWT_SECRET = new TextEncoder().encode('your-secret-key-should-be-at-least-32-chars-long');

// Token expiration time
const TOKEN_EXPIRY = '7d'; // 7 days

/**
 * Create a JWT token for a user
 */
export async function createToken(payload: Record<string, any>): Promise<string> {
  const jwt = await new jose.SignJWT(payload)
    .setProtectedHeader({ alg: 'HS256' })
    .setIssuedAt()
    .setExpirationTime(TOKEN_EXPIRY)
    .sign(JWT_SECRET);
  
  return jwt;
}

/**
 * Verify a JWT token and return the payload
 */
export async function verifyToken(token: string): Promise<Record<string, any> | null> {
  try {
    const { payload } = await jose.jwtVerify(token, JWT_SECRET);
    return payload as Record<string, any>;
  } catch (error) {
    console.error('Token verification failed:', error);
    return null;
  }
}

/**
 * Store a JWT token in localStorage
 */
export function storeToken(token: string): void {
  localStorage.setItem('auth_token', token);
}

/**
 * Get the JWT token from localStorage
 */
export function getToken(): string | null {
  return localStorage.getItem('auth_token');
}

/**
 * Remove the JWT token from localStorage
 */
export function removeToken(): void {
  localStorage.removeItem('auth_token');
}
