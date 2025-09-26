
import { createTheme } from '@mui/material/styles';

const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: { main: '#90caf9' },
    secondary: { main: '#f48fb1' },
    background: { default: '#0f172a', paper: '#111827' },
  },
  shape: { borderRadius: 12 },
});
export default theme;
