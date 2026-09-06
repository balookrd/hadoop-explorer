export interface UserSession {
  username: string;
  display_name: string;
  email?: string | null;
  groups: string[];
  auth_method?: 'ldap' | 'kerberos' | 'mock' | string;
  is_admin?: boolean;
  system_role?: 'reader' | 'writer' | 'admin' | string;
}

export interface AuthResponse {
  access_token: string;
  token_type?: string;
  user: UserSession;
}
