"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { Database, FileText, History, Plus } from "lucide-react";

const navItems = [
  { href: "/", label: "Dashboard", icon: Database },
  { href: "/sources", label: "Data Sources", icon: Database },
  { href: "/reconcile", label: "New Reconciliation", icon: Plus },
  { href: "/runs", label: "Run History", icon: History },
  { href: "/recipes", label: "Recipes", icon: FileText },
];

export function Navigation() {
  const pathname = usePathname();

  return (
    <header className="border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container mx-auto px-4">
        <div className="flex h-16 items-center justify-between">
          <div className="flex items-center gap-8">
            <Link href="/" className="flex items-center gap-2">
              <div className="h-8 w-8 rounded-lg bg-primary flex items-center justify-center">
                <span className="text-primary-foreground font-bold text-lg">K</span>
              </div>
              <span className="font-semibold text-xl">Kalla</span>
            </Link>
            <nav className="flex items-center gap-6">
              {navItems.map((item) => {
                const Icon = item.icon;
                const isActive = pathname === item.href ||
                  (item.href !== "/" && pathname.startsWith(item.href));
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      "flex items-center gap-2 text-sm font-medium transition-colors hover:text-primary",
                      isActive ? "text-primary" : "text-muted-foreground"
                    )}
                  >
                    <Icon className="h-4 w-4" />
                    {item.label}
                  </Link>
                );
              })}
            </nav>
          </div>
          <div className="flex items-center gap-4">
            <span className="text-xs text-muted-foreground">
              Universal Reconciliation Engine
            </span>
          </div>
        </div>
      </div>
    </header>
  );
}
