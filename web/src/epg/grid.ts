import type { Program, Service } from "../api";
import { epgServiceGroupKey, isStableEpgService, serviceKey } from "../domain/service";
import { normalizeProgramText } from "../domain/program";

export type EpgColumn = {
  key: string;
  primaryService: Service;
  services: Service[];
  foldedServices: Service[];
  isSubchannel: boolean;
};

export type EpgProgramBlock = {
  key: string;
  program: Program;
  service: Service;
};

const sharedProgramResolverCache = new WeakMap<Map<string, Program[]>, (program: Program) => string | undefined>();

export function makeEpgColumns(services: Service[], programsByService: Map<string, Program[]>): EpgColumn[] {
  const groups = new Map<string, Service[]>();
  const resolveSharedProgramKey = sharedProgramKeyResolverFor(programsByService);
  for (const service of services) {
    const key = epgServiceGroupKey(service);
    groups.set(key, [...(groups.get(key) ?? []), service]);
  }

  const columns: EpgColumn[] = [];
  for (const [groupKey, groupServices] of groups) {
    const primaryColumn: EpgColumn = {
      key: groupKey,
      primaryService: groupServices[0],
      services: [],
      foldedServices: [...groupServices],
      isSubchannel: false,
    };
    const seenContent = new Set<string>();
    const foldedServiceKeys = new Set<string>();
    let hasReadyBaseline = false;

    for (const [index, service] of groupServices.entries()) {
      const programs = programsByService.get(`${service.networkId}:${service.serviceId}`) ?? [];
      const contentEntries = programs.map((program) => ({
        key: programContentKey(program, resolveSharedProgramKey),
        sharedWithFoldedService: isSharedWithServiceSet(program, foldedServiceKeys),
      }));
      const canSplit = isStableEpgService(service) && hasReadyBaseline && contentEntries.length > 0;
      const hasDistinctContent = canSplit && contentEntries.some(({ key, sharedWithFoldedService }) => (
        !sharedWithFoldedService && !seenContent.has(key)
      ));

      if (index === 0 || hasDistinctContent) {
        const column = index === 0 ? primaryColumn : {
          key: `${groupKey}:service:${service.id}`,
          primaryService: service,
          services: [service],
          foldedServices: [service],
          isSubchannel: true,
        };
        if (index === 0) {
          primaryColumn.services.push(service);
          foldedServiceKeys.add(serviceKey(service));
        }
        columns.push(column);
      } else {
        primaryColumn.services.push(service);
        foldedServiceKeys.add(serviceKey(service));
      }

      if (isStableEpgService(service)) {
        hasReadyBaseline = true;
        for (const { key } of contentEntries) {
          seenContent.add(key);
        }
      }
    }
  }

  return columns;
}

export function makeEpgProgramBlocks(
  column: EpgColumn,
  programsByService: Map<string, Program[]>,
): EpgProgramBlock[] {
  const uniquePrograms = new Map<string, { program: Program; service: Service }>();
  const resolveSharedProgramKey = sharedProgramKeyResolverFor(programsByService);
  for (const service of column.services) {
    const programs = programsByService.get(`${service.networkId}:${service.serviceId}`) ?? [];
    for (const program of programs) {
      const key = programContentKey(program, resolveSharedProgramKey);
      if (!uniquePrograms.has(key)) {
        uniquePrograms.set(key, { program, service });
      }
    }
  }

  return [...uniquePrograms.values()]
    .sort((a, b) => a.program.startAt - b.program.startAt || a.program.id - b.program.id)
    .map(({ program, service }) => ({
      key: `${service.id}:${program.id}`,
      program,
      service,
    }));
}

export function programContentKey(program: Program, resolveSharedProgramKey?: (program: Program) => string | undefined) {
  const sharedKey = resolveSharedProgramKey?.(program);
  if (sharedKey) {
    return sharedKey;
  }
  return [
    program.startAt,
    program.duration,
    normalizeProgramText(program.name),
    normalizeProgramText(program.description),
  ].join(":");
}

export function makeSharedProgramKeyResolver(programs: Program[]) {
  const parent = new Map<string, string>();
  const canonical = new Map<string, string>();

  const find = (key: string): string => {
    const current = parent.get(key);
    if (current == null) {
      parent.set(key, key);
      return key;
    }
    if (current === key) {
      return key;
    }
    const root = find(current);
    parent.set(key, root);
    return root;
  };

  const union = (a: string, b: string) => {
    const rootA = find(a);
    const rootB = find(b);
    if (rootA === rootB) {
      return;
    }
    parent.set(rootB, rootA);
  };

  for (const program of programs) {
    const source = programEndpointKey(program.networkId, program.serviceId, program.eventId);
    for (const item of program.relatedItems ?? []) {
      if (item.type !== "shared" || item.serviceId == null || item.eventId == null) {
        continue;
      }
      union(source, programEndpointKey(item.networkId ?? program.networkId, item.serviceId, item.eventId));
    }
  }

  for (const key of parent.keys()) {
    const root = find(key);
    const current = canonical.get(root);
    if (current == null || key < current) {
      canonical.set(root, key);
    }
  }

  return (program: Program) => {
    const source = programEndpointKey(program.networkId, program.serviceId, program.eventId);
    const root = parent.get(source) == null ? undefined : find(source);
    if (root == null) {
      return undefined;
    }
    return `shared:${canonical.get(root) ?? root}`;
  };
}

function sharedProgramKeyResolverFor(programsByService: Map<string, Program[]>) {
  const cached = sharedProgramResolverCache.get(programsByService);
  if (cached) {
    return cached;
  }
  const resolver = makeSharedProgramKeyResolver(flattenProgramGroups(programsByService));
  sharedProgramResolverCache.set(programsByService, resolver);
  return resolver;
}

function flattenProgramGroups(programsByService: Map<string, Program[]>) {
  return [...programsByService.values()].flat();
}

function programEndpointKey(networkId: number, serviceId: number, eventId: number) {
  return `${networkId}:${serviceId}:${eventId}`;
}

function isSharedWithServiceSet(program: Program, serviceKeys: Set<string>) {
  return (program.relatedItems ?? []).some((item) => (
    item.type === "shared" &&
    item.serviceId != null &&
    serviceKeys.has(serviceKey({
      networkId: item.networkId ?? program.networkId,
      serviceId: item.serviceId,
    }))
  ));
}
