import { createBrowserRouter } from "react-router-dom";
import { Layout } from "@/components/layout";
import { LedgerServicesListPage } from "@/pages/ledger-services-list";
import { LedgerServiceDetailPage } from "@/pages/ledger-service-detail";
import { LedgerServiceFormPage } from "@/pages/ledger-service-form";
import { LedgerDefaultsListPage } from "@/pages/ledger-defaults-list";
import { LedgerDefaultsDetailPage } from "@/pages/ledger-defaults-detail";
import { LedgerDefaultsFormPage } from "@/pages/ledger-defaults-form";

export const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      {
        path: "/",
        element: <LedgerServicesListPage />,
      },
      {
        path: "/namespaces/:ns/ledger-services",
        element: <LedgerServicesListPage />,
      },
      {
        path: "/namespaces/:ns/ledger-services/new",
        element: <LedgerServiceFormPage />,
      },
      {
        path: "/namespaces/:ns/ledger-services/:name",
        element: <LedgerServiceDetailPage />,
      },
      {
        path: "/namespaces/:ns/ledger-services/:name/edit",
        element: <LedgerServiceFormPage />,
      },
      {
        path: "/ledger-defaults",
        element: <LedgerDefaultsListPage />,
      },
      {
        path: "/ledger-defaults/new",
        element: <LedgerDefaultsFormPage />,
      },
      {
        path: "/ledger-defaults/:name",
        element: <LedgerDefaultsDetailPage />,
      },
      {
        path: "/ledger-defaults/:name/edit",
        element: <LedgerDefaultsFormPage />,
      },
    ],
  },
]);
