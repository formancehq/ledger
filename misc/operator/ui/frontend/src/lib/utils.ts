import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatAge(timestamp: string | undefined): string {
  if (!timestamp) return "-";
  const ms = Date.now() - new Date(timestamp).getTime();
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;
  const days = Math.floor(hours / 24);
  return `${days}d`;
}

export function formatImage(image?: {
  repository?: string;
  tag?: string;
}): string {
  if (!image) return "-";
  if (!image.repository && !image.tag) return "(from defaults)";
  if (!image.repository) return `:${image.tag}`;
  if (!image.tag) return image.repository;
  return `${image.repository}:${image.tag}`;
}

export function phaseColor(phase?: string): string {
  switch (phase) {
    case "Running":
      return "bg-green-100 text-green-800";
    case "Degraded":
      return "bg-yellow-100 text-yellow-800";
    default:
      return "bg-gray-100 text-gray-600";
  }
}

const ERROR_STATUSES = new Set([
  "ImagePullBackOff",
  "ErrImagePull",
  "CrashLoopBackOff",
  "CreateContainerConfigError",
  "InvalidImageName",
  "RunContainerError",
  "OOMKilled",
  "Error",
]);

const WARNING_STATUSES = new Set([
  "Pending",
  "ContainerCreating",
  "PodInitializing",
  "Terminating",
]);

export function podStatusColor(status: string): string {
  if (status === "Running" || status === "Completed" || status === "Succeeded") {
    return "bg-green-100 text-green-800";
  }
  // Check if it starts with Init: (init container errors)
  const base = status.startsWith("Init:") ? status.slice(5) : status;
  if (ERROR_STATUSES.has(base) || status.startsWith("ExitCode:") || status.startsWith("Signal:")) {
    return "bg-red-100 text-red-800";
  }
  if (WARNING_STATUSES.has(base)) {
    return "bg-yellow-100 text-yellow-800";
  }
  return "bg-gray-100 text-gray-600";
}

export function eventTypeColor(type: string): string {
  switch (type) {
    case "Warning":
      return "bg-yellow-100 text-yellow-800";
    case "Normal":
      return "bg-blue-100 text-blue-800";
    default:
      return "bg-gray-100 text-gray-600";
  }
}
