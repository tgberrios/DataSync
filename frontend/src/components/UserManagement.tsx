import { useState, useEffect, useCallback, useRef } from 'react';
import { format } from 'date-fns';
import {
  Container,
  Header,
  FiltersContainer,
  Select,
  TableContainer,
  Table,
  Th,
  Td,
  TableRow,
  Pagination,
  PageButton,
  ErrorMessage,
  LoadingOverlay,
  Input,
  Button,
  ActiveBadge,
  ActionButton,
  FormGroup,
  Label,
  SearchInput,
  SearchButton,
  ClearSearchButton,
  PaginationInfo,
  SearchContainer,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalTitle,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { authApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
import styled from 'styled-components';
import { theme } from '../theme/theme';
import UserManagementTreeView from './UserManagementTreeView';

const HeaderContent = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
`;

const CloseButton = styled.button`
  background: none;
  border: none;
  font-size: 1.5em;
  cursor: pointer;
  color: ${theme.colors.text.secondary};
  padding: 0;
  width: 30px;
  height: 30px;
  display: flex;
  align-items: center;
  justify-content: center;
  
  &:hover {
    color: ${theme.colors.text.primary};
  }
`;

const ButtonGroup = styled.div`
  display: flex;
  gap: ${theme.spacing.md};
  justify-content: flex-end;
  margin-top: ${theme.spacing.lg};
  padding-top: ${theme.spacing.md};
  border-top: 1px solid ${theme.colors.border.light};
`;

const RoleBadge = styled.span<{ $role: string }>`
  padding: 4px 12px;
  border-radius: 6px;
  font-size: 0.9em;
  font-weight: 500;
  display: inline-block;
  background-color: ${props => {
    switch (props.$role) {
      case 'admin': return '#ffebee';
      case 'user': return '#e3f2fd';
      case 'viewer': return '#f3e5f5';
      default: return '#f5f5f5';
    }
  }};
  color: ${props => {
    switch (props.$role) {
      case 'admin': return '#c62828';
      case 'user': return '#1565c0';
      case 'viewer': return '#6a1b9a';
      default: return '#757575';
    }
  }};
`;

interface User {
  id: number;
  username: string;
  email: string;
  role: 'admin' | 'user' | 'viewer';
  active: boolean;
  created_at: string;
  updated_at: string;
  last_login: string | null;
}

const UserManagement = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter } = useTableFilters({
    role: '',
    active: ''
  });
  
  const [searchInput, setSearchInput] = useState('');
  const [search, setSearch] = useState('');
  const [data, setData] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    page: 1,
    limit: 20
  });
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isPasswordModalOpen, setIsPasswordModalOpen] = useState(false);
  const [editingUser, setEditingUser] = useState<User | null>(null);
  const [passwordUserId, setPasswordUserId] = useState<number | null>(null);
  const [userForm, setUserForm] = useState({
    username: '',
    email: '',
    password: '',
    role: 'user' as 'admin' | 'user' | 'viewer',
    active: true
  });
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [selectedUser, setSelectedUser] = useState<User | null>(null);
  const [viewMode, setViewMode] = useState<'table' | 'tree'>('tree');
  const isMountedRef = useRef(true);

  const fetchUsers = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(search, 100);
      const params: any = {
        page: viewMode === 'tree' ? 1 : page,
        limit: viewMode === 'tree' ? 1000 : limit,
        search: sanitizedSearch
      };
      
      if (filters.role) params.role = filters.role;
      if (filters.active !== '') params.active = filters.active;
      
      const response = await authApi.getUsers(params);
      if (isMountedRef.current) {
        setData(response.data || []);
        setPagination(response.pagination || {
          total: 0,
          totalPages: 0,
          page: 1,
          limit: 20
        });
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [page, limit, filters.role, filters.active, search, viewMode]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchUsers();
    return () => {
      isMountedRef.current = false;
    };
  }, [fetchUsers]);

  const handleSearch = useCallback(() => {
    setSearch(searchInput);
    setPage(1);
  }, [searchInput, setPage]);

  const handleClearSearch = useCallback(() => {
    setSearchInput('');
    setSearch('');
    setPage(1);
  }, [setPage]);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    setPage(1);
  }, [setFilter, setPage]);

  const handleOpenModal = useCallback((user?: User) => {
    if (user) {
      setEditingUser(user);
      setUserForm({
        username: user.username,
        email: user.email,
        password: '',
        role: user.role,
        active: user.active
      });
    } else {
      setEditingUser(null);
      setUserForm({
        username: '',
        email: '',
        password: '',
        role: 'user',
        active: true
      });
    }
    setIsModalOpen(true);
  }, []);

  const handleCloseModal = useCallback(() => {
    setIsModalOpen(false);
    setEditingUser(null);
  }, []);

  const handleSave = useCallback(async () => {
    try {
      if (editingUser) {
        await authApi.updateUser(editingUser.id, {
          username: userForm.username,
          email: userForm.email,
          role: userForm.role,
          active: userForm.active
        });
      } else {
        if (!userForm.password || userForm.password.length < 8) {
          setError('Password must be at least 8 characters long');
          return;
        }
        await authApi.createUser(
          userForm.username,
          userForm.email,
          userForm.password,
          userForm.role
        );
      }
      handleCloseModal();
      fetchUsers();
    } catch (err) {
      setError(extractApiError(err));
    }
  }, [editingUser, userForm, handleCloseModal, fetchUsers]);

  const handleDelete = useCallback(async (userId: number, username: string) => {
    if (!confirm(`Are you sure you want to delete user "${username}"?`)) {
      return;
    }
    try {
      await authApi.deleteUser(userId);
      fetchUsers();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchUsers]);

  const handleToggleActive = useCallback(async (userId: number, currentActive: boolean) => {
    try {
      await authApi.updateUser(userId, { active: !currentActive });
      fetchUsers();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchUsers]);

  const handleOpenPasswordModal = useCallback((userId: number) => {
    setPasswordUserId(userId);
    setNewPassword('');
    setConfirmPassword('');
    setIsPasswordModalOpen(true);
  }, []);

  const handleClosePasswordModal = useCallback(() => {
    setIsPasswordModalOpen(false);
    setPasswordUserId(null);
    setNewPassword('');
    setConfirmPassword('');
  }, []);

  const handleResetPassword = useCallback(async () => {
    if (!passwordUserId) return;
    
    if (!newPassword || newPassword.length < 8) {
      setError('Password must be at least 8 characters long');
      return;
    }
    
    if (newPassword !== confirmPassword) {
      setError('Passwords do not match');
      return;
    }
    
    try {
      await authApi.resetUserPassword(passwordUserId, newPassword);
      handleClosePasswordModal();
      alert('Password reset successfully');
    } catch (err) {
      setError(extractApiError(err));
    }
  }, [passwordUserId, newPassword, confirmPassword, handleClosePasswordModal]);

  return (
    <Container>
      <Header>
        <HeaderContent>
          <span>User Management</span>
          <Button $variant="primary" onClick={() => handleOpenModal()}>
            + Add User
          </Button>
        </HeaderContent>
      </Header>

      {loading && <LoadingOverlay>Loading users...</LoadingOverlay>}
      {error && <ErrorMessage>{error}</ErrorMessage>}

      <FiltersContainer>
        <SearchContainer>
          <SearchInput
            type="text"
            placeholder="Search by username or email..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
          />
          <SearchButton onClick={handleSearch}>Search</SearchButton>
          {search && (
            <ClearSearchButton onClick={handleClearSearch}>Clear</ClearSearchButton>
          )}
        </SearchContainer>

        <Select
          value={filters.role}
          onChange={(e) => handleFilterChange('role', e.target.value)}
        >
          <option value="">All Roles</option>
          <option value="admin">Admin</option>
          <option value="user">User</option>
          <option value="viewer">Viewer</option>
        </Select>

        <Select
          value={filters.active}
          onChange={(e) => handleFilterChange('active', e.target.value)}
        >
          <option value="">All Status</option>
          <option value="true">Active</option>
          <option value="false">Inactive</option>
        </Select>
      </FiltersContainer>

      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: theme.spacing.md }}>
        <div style={{ display: 'flex', gap: theme.spacing.sm }}>
          <Button
            $variant={viewMode === 'tree' ? 'primary' : 'secondary'}
            onClick={() => setViewMode('tree')}
            style={{ padding: '8px 16px', fontSize: '0.9em' }}
          >
            Tree View
          </Button>
          <Button
            $variant={viewMode === 'table' ? 'primary' : 'secondary'}
            onClick={() => setViewMode('table')}
            style={{ padding: '8px 16px', fontSize: '0.9em' }}
          >
            Table View
          </Button>
        </div>
      </div>

      {viewMode === 'tree' ? (
        <div style={{ display: 'grid', gridTemplateColumns: selectedUser ? '1fr 400px' : '1fr', gap: theme.spacing.lg }}>
          <UserManagementTreeView 
            users={data} 
            onUserClick={(user) => setSelectedUser(prev => prev?.id === user.id ? null : user)}
          />
          
          {selectedUser && (
            <div style={{
              background: theme.colors.background.secondary,
              border: `1px solid ${theme.colors.border.light}`,
              borderRadius: theme.borderRadius.md,
              padding: theme.spacing.lg,
              position: 'sticky',
              top: theme.spacing.md,
              maxHeight: 'calc(100vh - 200px)',
              overflowY: 'auto'
            }}>
              <h3 style={{ marginTop: 0, marginBottom: theme.spacing.md, color: theme.colors.text.primary }}>
                User Details
              </h3>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: theme.spacing.md }}>
                <div>
                  <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Username:</strong>
                  <div style={{ color: theme.colors.text.primary, fontSize: '0.9em', marginTop: '4px' }}>
                    {selectedUser.username}
                  </div>
                </div>
                <div>
                  <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Email:</strong>
                  <div style={{ color: theme.colors.text.primary, fontSize: '0.9em', marginTop: '4px' }}>
                    {selectedUser.email}
                  </div>
                </div>
                <div>
                  <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Role:</strong>
                  <div style={{ marginTop: '4px' }}>
                    <RoleBadge $role={selectedUser.role}>{selectedUser.role}</RoleBadge>
                  </div>
                </div>
                <div>
                  <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Status:</strong>
                  <div style={{ marginTop: '4px' }}>
                    <ActiveBadge $active={selectedUser.active}>
                      {selectedUser.active ? 'Active' : 'Inactive'}
                    </ActiveBadge>
                  </div>
                </div>
                <div>
                  <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Created:</strong>
                  <div style={{ color: theme.colors.text.primary, fontSize: '0.9em', marginTop: '4px' }}>
                    {format(new Date(selectedUser.created_at), 'yyyy-MM-dd HH:mm')}
                  </div>
                </div>
                <div>
                  <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Last Login:</strong>
                  <div style={{ color: theme.colors.text.primary, fontSize: '0.9em', marginTop: '4px' }}>
                    {selectedUser.last_login
                      ? format(new Date(selectedUser.last_login), 'yyyy-MM-dd HH:mm')
                      : 'Never'}
                  </div>
                </div>
                <div style={{ marginTop: theme.spacing.md, paddingTop: theme.spacing.md, borderTop: `1px solid ${theme.colors.border.light}` }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: theme.spacing.sm }}>
                    <ActionButton onClick={() => {
                      handleOpenModal(selectedUser);
                      setSelectedUser(null);
                    }}>
                      Edit User
                    </ActionButton>
                    <ActionButton
                      $variant="danger"
                      onClick={() => {
                        handleToggleActive(selectedUser.id, selectedUser.active);
                        setSelectedUser(null);
                      }}
                    >
                      {selectedUser.active ? 'Deactivate' : 'Activate'}
                    </ActionButton>
                    <ActionButton
                      onClick={() => {
                        handleOpenPasswordModal(selectedUser.id);
                        setSelectedUser(null);
                      }}
                    >
                      Reset Password
                    </ActionButton>
                    <ActionButton
                      $variant="danger"
                      onClick={() => {
                        handleDelete(selectedUser.id, selectedUser.username);
                        setSelectedUser(null);
                      }}
                    >
                      Delete User
                    </ActionButton>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      ) : (
        <>
          <TableContainer>
        <Table>
          <thead>
            <tr>
              <Th>Username</Th>
              <Th>Email</Th>
              <Th>Role</Th>
              <Th>Status</Th>
              <Th>Created</Th>
              <Th>Last Login</Th>
              <Th>Actions</Th>
            </tr>
          </thead>
          <tbody>
            {data.map((user) => (
              <TableRow key={user.id}>
                <Td>{user.username}</Td>
                <Td>{user.email}</Td>
                <Td>
                  <RoleBadge $role={user.role}>{user.role}</RoleBadge>
                </Td>
                <Td>
                  <ActiveBadge $active={user.active}>
                    {user.active ? 'Active' : 'Inactive'}
                  </ActiveBadge>
                </Td>
                <Td>{format(new Date(user.created_at), 'yyyy-MM-dd HH:mm')}</Td>
                <Td>
                  {user.last_login
                    ? format(new Date(user.last_login), 'yyyy-MM-dd HH:mm')
                    : 'Never'}
                </Td>
                <Td>
                  <ActionButton onClick={() => handleOpenModal(user)}>Edit</ActionButton>
                  <ActionButton
                    $variant="danger"
                    onClick={() => handleToggleActive(user.id, user.active)}
                  >
                    {user.active ? 'Deactivate' : 'Activate'}
                  </ActionButton>
                  <ActionButton
                    onClick={() => handleOpenPasswordModal(user.id)}
                  >
                    Reset Pwd
                  </ActionButton>
                  <ActionButton
                    $variant="danger"
                    onClick={() => handleDelete(user.id, user.username)}
                  >
                    Delete
                  </ActionButton>
                </Td>
              </TableRow>
            ))}
          </tbody>
        </Table>
      </TableContainer>

      <PaginationInfo>
        Showing {data.length > 0 ? (page - 1) * limit + 1 : 0} to{' '}
        {Math.min(page * limit, pagination.total)} of {pagination.total} users
      </PaginationInfo>

      {pagination.totalPages > 1 && (
        <Pagination>
          <PageButton
            disabled={page === 1}
            onClick={() => setPage(page - 1)}
          >
            Previous
          </PageButton>
          <span>
            Page {page} of {pagination.totalPages}
          </span>
          <PageButton
            disabled={page === pagination.totalPages}
            onClick={() => setPage(page + 1)}
          >
            Next
          </PageButton>
          </Pagination>
        )}
        </>
      )}

      {isModalOpen && (
        <ModalOverlay $isOpen={isModalOpen} onClick={handleCloseModal}>
          <ModalContent onClick={(e) => e.stopPropagation()}>
            <ModalHeader>
              <ModalTitle>
                {editingUser ? 'Edit User' : 'Create User'}
              </ModalTitle>
              <CloseButton onClick={handleCloseModal}>×</CloseButton>
            </ModalHeader>
            <FormGroup>
              <Label htmlFor="username">Username *</Label>
              <Input
                id="username"
                type="text"
                value={userForm.username}
                onChange={(e) =>
                  setUserForm({ ...userForm, username: e.target.value })
                }
                required
                disabled={!!editingUser}
              />
            </FormGroup>
            <FormGroup>
              <Label htmlFor="email">Email *</Label>
              <Input
                id="email"
                type="email"
                value={userForm.email}
                onChange={(e) =>
                  setUserForm({ ...userForm, email: e.target.value })
                }
                required
              />
            </FormGroup>
            {!editingUser && (
              <FormGroup>
                <Label htmlFor="password">Password * (min 8 characters)</Label>
                <Input
                  id="password"
                  type="password"
                  value={userForm.password}
                  onChange={(e) =>
                    setUserForm({ ...userForm, password: e.target.value })
                  }
                  required
                />
              </FormGroup>
            )}
            <FormGroup>
              <Label htmlFor="role">Role *</Label>
              <Select
                id="role"
                value={userForm.role}
                onChange={(e) =>
                  setUserForm({
                    ...userForm,
                    role: e.target.value as 'admin' | 'user' | 'viewer',
                  })
                }
              >
                <option value="viewer">Viewer</option>
                <option value="user">User</option>
                <option value="admin">Admin</option>
              </Select>
            </FormGroup>
            <FormGroup>
              <Label>
                <input
                  type="checkbox"
                  checked={userForm.active}
                  onChange={(e) =>
                    setUserForm({ ...userForm, active: e.target.checked })
                  }
                />{' '}
                Active
              </Label>
            </FormGroup>
            <ButtonGroup>
              <Button onClick={handleCloseModal}>Cancel</Button>
              <Button $variant="primary" onClick={handleSave}>
                {editingUser ? 'Update' : 'Create'}
              </Button>
            </ButtonGroup>
          </ModalContent>
        </ModalOverlay>
      )}

      {isPasswordModalOpen && (
        <ModalOverlay $isOpen={isPasswordModalOpen} onClick={handleClosePasswordModal}>
          <ModalContent onClick={(e) => e.stopPropagation()}>
            <ModalHeader>
              <ModalTitle>Reset Password</ModalTitle>
              <CloseButton onClick={handleClosePasswordModal}>×</CloseButton>
            </ModalHeader>
            <FormGroup>
              <Label htmlFor="newPassword">New Password * (min 8 characters)</Label>
              <Input
                id="newPassword"
                type="password"
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                required
              />
            </FormGroup>
            <FormGroup>
              <Label htmlFor="confirmPassword">Confirm Password *</Label>
              <Input
                id="confirmPassword"
                type="password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                required
              />
            </FormGroup>
            <ButtonGroup>
              <Button onClick={handleClosePasswordModal}>Cancel</Button>
              <Button $variant="primary" onClick={handleResetPassword}>
                Reset Password
              </Button>
            </ButtonGroup>
          </ModalContent>
        </ModalOverlay>
      )}
    </Container>
  );
};

export default UserManagement;

