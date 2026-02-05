'use client';

import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { cn } from '@/lib/utils';
import type { Components } from 'react-markdown';

interface MarkdownRendererProps {
  content: string;
  className?: string;
}

// Custom component overrides for styling
const components: Components = {
  // Open links in new tab with security attributes
  a: ({ href, children, ...props }) => (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="text-primary underline underline-offset-2 hover:text-primary/80"
      {...props}
    >
      {children}
    </a>
  ),
  // Style inline code
  code: ({ children, className, ...props }) => {
    // Check if it's a code block (has language class) vs inline code
    const isBlock = className?.includes('language-');
    if (isBlock) {
      return (
        <code
          className={cn(
            'block bg-muted rounded-md p-3 text-xs overflow-x-auto',
            className
          )}
          {...props}
        >
          {children}
        </code>
      );
    }
    return (
      <code
        className="bg-muted px-1.5 py-0.5 rounded text-xs font-mono"
        {...props}
      >
        {children}
      </code>
    );
  },
  // Style code blocks wrapper
  pre: ({ children, ...props }) => (
    <pre className="bg-muted rounded-md overflow-x-auto" {...props}>
      {children}
    </pre>
  ),
  // Reduce heading sizes for chat context
  h1: ({ children, ...props }) => (
    <h1 className="text-lg font-semibold mt-3 mb-1" {...props}>{children}</h1>
  ),
  h2: ({ children, ...props }) => (
    <h2 className="text-base font-semibold mt-2 mb-1" {...props}>{children}</h2>
  ),
  h3: ({ children, ...props }) => (
    <h3 className="text-sm font-semibold mt-2 mb-1" {...props}>{children}</h3>
  ),
  // Style lists
  ul: ({ children, ...props }) => (
    <ul className="list-disc list-inside space-y-0.5 my-1" {...props}>{children}</ul>
  ),
  ol: ({ children, ...props }) => (
    <ol className="list-decimal list-inside space-y-0.5 my-1" {...props}>{children}</ol>
  ),
  li: ({ children, ...props }) => (
    <li className="text-sm" {...props}>{children}</li>
  ),
  // Style paragraphs
  p: ({ children, ...props }) => (
    <p className="my-1 leading-relaxed" {...props}>{children}</p>
  ),
  // Style blockquotes
  blockquote: ({ children, ...props }) => (
    <blockquote
      className="border-l-2 border-muted-foreground/30 pl-3 italic my-2"
      {...props}
    >
      {children}
    </blockquote>
  ),
  // Style horizontal rules
  hr: ({ ...props }) => (
    <hr className="border-muted-foreground/20 my-3" {...props} />
  ),
  // Style tables
  table: ({ children, ...props }) => (
    <div className="overflow-x-auto my-2">
      <table className="min-w-full text-xs border-collapse" {...props}>
        {children}
      </table>
    </div>
  ),
  th: ({ children, ...props }) => (
    <th className="border border-muted-foreground/20 px-2 py-1 bg-muted font-medium text-left" {...props}>
      {children}
    </th>
  ),
  td: ({ children, ...props }) => (
    <td className="border border-muted-foreground/20 px-2 py-1" {...props}>
      {children}
    </td>
  ),
};

export function MarkdownRenderer({ content, className }: MarkdownRendererProps) {
  return (
    <div className={cn('text-sm', className)}>
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
        {content}
      </ReactMarkdown>
    </div>
  );
}
