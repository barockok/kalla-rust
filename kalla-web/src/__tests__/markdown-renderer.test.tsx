import { render, screen } from '@testing-library/react';
import { MarkdownRenderer } from '@/components/chat/MarkdownRenderer';

describe('MarkdownRenderer', () => {
  test('renders plain text', () => {
    render(<MarkdownRenderer content="Hello world" />);
    expect(screen.getByText('Hello world')).toBeInTheDocument();
  });

  test('renders bold text as strong element', () => {
    render(<MarkdownRenderer content="This is **bold** text" />);
    const strong = screen.getByText('bold');
    expect(strong.tagName).toBe('STRONG');
  });

  test('renders italic text as em element', () => {
    render(<MarkdownRenderer content="This is *italic* text" />);
    const em = screen.getByText('italic');
    expect(em.tagName).toBe('EM');
  });

  test('renders bullet lists as ul/li elements', () => {
    render(<MarkdownRenderer content="- Item 1\n- Item 2\n- Item 3" />);
    const items = screen.getAllByRole('listitem');
    expect(items).toHaveLength(3);
  });

  test('renders numbered lists as ol/li elements', () => {
    render(<MarkdownRenderer content="1. First\n2. Second\n3. Third" />);
    const list = screen.getByRole('list');
    expect(list.tagName).toBe('OL');
  });

  test('renders inline code with code element', () => {
    render(<MarkdownRenderer content="Use `npm install` to install" />);
    const code = screen.getByText('npm install');
    expect(code.tagName).toBe('CODE');
  });

  test('renders links as anchor elements', () => {
    render(<MarkdownRenderer content="Visit [Google](https://google.com)" />);
    const link = screen.getByRole('link', { name: 'Google' });
    expect(link).toHaveAttribute('href', 'https://google.com');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('renders headings correctly', () => {
    render(<MarkdownRenderer content="## Section Title" />);
    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toHaveTextContent('Section Title');
  });

  test('applies custom className', () => {
    const { container } = render(
      <MarkdownRenderer content="Test" className="custom-class" />
    );
    expect(container.firstChild).toHaveClass('custom-class');
  });
});
