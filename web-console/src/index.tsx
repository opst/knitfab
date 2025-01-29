import { createRoot } from 'react-dom/client';
import App from './app';

const app = document.getElementById('app');
if (app !== null) {
    const root = createRoot(app);
    root.render(<App apiRoot="/api" />);
}
