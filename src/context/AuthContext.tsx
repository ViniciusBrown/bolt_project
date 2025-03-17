import React, { createContext, useContext, useState, useEffect } from 'react';
import { User, AuthState } from '@/types';
import { createToken, verifyToken, storeToken, getToken, removeToken } from '@/lib/jwt';

interface AuthContextType extends AuthState {
  login: (email: string, password: string) => Promise<void>;
  signup: (email: string, password: string, name: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

// Mock user database key
const USERS_STORAGE_KEY = 'twitter_sentiment_users';

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [authState, setAuthState] = useState<AuthState>({
    user: null,
    isAuthenticated: false,
    isLoading: true,
  });

  // Load user from JWT token on initial render
  useEffect(() => {
    const loadUserFromToken = async () => {
      const token = getToken();
      
      if (!token) {
        setAuthState({
          user: null,
          isAuthenticated: false,
          isLoading: false,
        });
        return;
      }
      
      const payload = await verifyToken(token);
      
      if (!payload) {
        // Token is invalid or expired
        removeToken();
        setAuthState({
          user: null,
          isAuthenticated: false,
          isLoading: false,
        });
        return;
      }
      
      // Token is valid, extract user data
      const user: User = {
        id: payload.id as string,
        email: payload.email as string,
        name: payload.name as string,
        avatarUrl: payload.avatarUrl as string,
      };
      
      setAuthState({
        user,
        isAuthenticated: true,
        isLoading: false,
      });
    };

    loadUserFromToken();
  }, []);

  // Generate a placeholder avatar URL based on name
  const generateAvatarUrl = (name: string): string => {
    // Use a placeholder service that generates avatars based on initials
    const initials = name
      .split(' ')
      .map(part => part[0])
      .join('')
      .toUpperCase()
      .substring(0, 2);
    
    // Generate a random color based on the name
    const colors = [
      '1abc9c', '2ecc71', '3498db', '9b59b6', '34495e',
      'f1c40f', 'e67e22', 'e74c3c', 'ecf0f1', '95a5a6'
    ];
    
    const colorIndex = name.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0) % colors.length;
    const color = colors[colorIndex];
    
    return `https://ui-avatars.com/api/?name=${encodeURIComponent(initials)}&background=${color}&color=fff`;
  };

  const login = async (email: string, password: string) => {
    // In a real app, you would make an API call here
    // For this demo, we'll simulate a successful login
    
    // Simulate API delay
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Check if user exists (in a real app, this would be done server-side)
    const storedUsers = localStorage.getItem(USERS_STORAGE_KEY) || '[]';
    const users = JSON.parse(storedUsers);
    
    const user = users.find((u: any) => u.email === email);
    
    if (!user) {
      throw new Error('User not found');
    }
    
    if (user.password !== password) {
      throw new Error('Invalid password');
    }
    
    // Create user object without password
    const authenticatedUser: User = {
      id: user.id,
      email: user.email,
      name: user.name,
      avatarUrl: user.avatarUrl
    };
    
    // Create and store JWT token
    const token = await createToken({
      id: authenticatedUser.id,
      email: authenticatedUser.email,
      name: authenticatedUser.name,
      avatarUrl: authenticatedUser.avatarUrl
    });
    
    storeToken(token);
    
    // Update state
    setAuthState({
      user: authenticatedUser,
      isAuthenticated: true,
      isLoading: false,
    });
  };

  const signup = async (email: string, password: string, name: string) => {
    // In a real app, you would make an API call here
    // For this demo, we'll simulate a successful signup
    
    // Simulate API delay
    await new Promise(resolve => setTimeout(resolve, 500));
    
    // Check if user already exists
    const storedUsers = localStorage.getItem(USERS_STORAGE_KEY) || '[]';
    const users = JSON.parse(storedUsers);
    
    if (users.some((u: any) => u.email === email)) {
      throw new Error('User already exists');
    }
    
    // Generate avatar URL
    const avatarUrl = generateAvatarUrl(name);
    
    // Create new user
    const newUser = {
      id: `user_${Date.now()}`,
      email,
      password, // In a real app, this would be hashed
      name,
      avatarUrl
    };
    
    // Save to "database"
    users.push(newUser);
    localStorage.setItem(USERS_STORAGE_KEY, JSON.stringify(users));
    
    // Create user object without password
    const authenticatedUser: User = {
      id: newUser.id,
      email: newUser.email,
      name: newUser.name,
      avatarUrl: newUser.avatarUrl
    };
    
    // Create and store JWT token
    const token = await createToken({
      id: authenticatedUser.id,
      email: authenticatedUser.email,
      name: authenticatedUser.name,
      avatarUrl: authenticatedUser.avatarUrl
    });
    
    storeToken(token);
    
    // Update state
    setAuthState({
      user: authenticatedUser,
      isAuthenticated: true,
      isLoading: false,
    });
  };

  const logout = () => {
    // Remove JWT token
    removeToken();
    
    // Update state
    setAuthState({
      user: null,
      isAuthenticated: false,
      isLoading: false,
    });
  };

  return (
    <AuthContext.Provider
      value={{
        ...authState,
        login,
        signup,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
