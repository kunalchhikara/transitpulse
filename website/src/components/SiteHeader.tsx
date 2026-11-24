"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { NAV_ITEMS } from "@/lib/navigation";

/**
 * Primary site navigation.
 *
 * There is a single <nav> landmark and a single list of links — the mobile
 * "hamburger" button only toggles CSS visibility of that one list (hidden
 * below `md`, always shown at `md` and up). Deliberately not duplicating
 * the link markup for mobile vs desktop keeps there being exactly one
 * accessible path to each page, and keeps route/nav tests from having to
 * disambiguate duplicate links.
 */
export function SiteHeader() {
  const pathname = usePathname();
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <header className="border-b border-foreground/10 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/75">
      <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-2 px-4 py-3 sm:px-6 lg:px-8">
        <Link
          href="/"
          className="flex items-center gap-2 text-lg font-bold tracking-tight text-foreground"
        >
          <span
            aria-hidden="true"
            className="inline-block h-2.5 w-2.5 rounded-full bg-sky-500"
          />
          TransitPulse
        </Link>

        <button
          type="button"
          className="inline-flex items-center justify-center rounded-md border border-foreground/20 p-2 text-foreground md:hidden"
          aria-expanded={mobileOpen}
          aria-controls="primary-navigation"
          onClick={() => setMobileOpen((open) => !open)}
        >
          <span className="sr-only">Toggle site navigation</span>
          <svg
            aria-hidden="true"
            viewBox="0 0 20 20"
            fill="currentColor"
            className="h-5 w-5"
          >
            {mobileOpen ? (
              <path d="M5.47 5.47a.75.75 0 0 1 1.06 0L10 8.94l3.47-3.47a.75.75 0 1 1 1.06 1.06L11.06 10l3.47 3.47a.75.75 0 1 1-1.06 1.06L10 11.06l-3.47 3.47a.75.75 0 0 1-1.06-1.06L8.94 10 5.47 6.53a.75.75 0 0 1 0-1.06Z" />
            ) : (
              <path
                fillRule="evenodd"
                d="M3 5.75A.75.75 0 0 1 3.75 5h12.5a.75.75 0 0 1 0 1.5H3.75A.75.75 0 0 1 3 5.75Zm0 4.5A.75.75 0 0 1 3.75 9.5h12.5a.75.75 0 0 1 0 1.5H3.75A.75.75 0 0 1 3 10.25Zm0 4.5a.75.75 0 0 1 .75-.75h12.5a.75.75 0 0 1 0 1.5H3.75a.75.75 0 0 1-.75-.75Z"
                clipRule="evenodd"
              />
            )}
          </svg>
        </button>

        <nav aria-label="Primary" className="order-last w-full md:order-none md:w-auto">
          <ul
            id="primary-navigation"
            className={`${mobileOpen ? "flex" : "hidden"} w-full flex-col gap-1 border-t border-foreground/10 py-3 text-sm font-medium md:flex md:w-auto md:flex-row md:items-center md:border-t-0 md:py-0`}
          >
            {NAV_ITEMS.map((item) => {
              const isActive = pathname === item.href;
              return (
                <li key={item.href}>
                  <Link
                    href={item.href}
                    aria-current={isActive ? "page" : undefined}
                    onClick={() => setMobileOpen(false)}
                    className={`block rounded-md px-3 py-2 transition-colors hover:bg-foreground/10 ${
                      isActive
                        ? "bg-foreground/10 text-foreground"
                        : "text-foreground/70"
                    }`}
                  >
                    {item.label}
                  </Link>
                </li>
              );
            })}
          </ul>
        </nav>
      </div>
    </header>
  );
}
