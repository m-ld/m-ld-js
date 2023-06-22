import { concatIter, mapIter } from '../engine/util';
import { TSeqCharTick, TSeqName, TSeqOperation, TSeqRun } from './types';
import { TSeqCharNode } from './TSeqNode';

export interface TSeqOperable {
  path: TSeqName[],
  charTick: TSeqCharTick,
  /** Contiguous next – if undefined, nothing contiguous in context */
  next?: TSeqOperable
}

export namespace TSeqOperable {
  /**
   * Processes the given operables into a set of "runs", which comprise an
   * anchor path and contiguous char-ticks following it
   */
  export function *toRuns(nodes: Iterable<TSeqOperable>): Iterable<TSeqRun> {
    const anchors = new Map(mapIter(nodes, node => [node, [] as TSeqCharTick[]]));
    for (let [node, run] of anchors) {
      for (let next = node.next; next != null && anchors.has(next); next = next.next) {
        run.push(next.charTick, ...anchors.get(next)!);
        anchors.delete(next);
      }
    }
    for (let [node, run] of anchors) {
      run.unshift(node.charTick);
      yield [node.path, run];
    }
  }

  class TSeqOperationTree {
    root: { [pid: string]: { [index: number]: TSeqPathTreeNode } } = {};
    operables = new Map<TSeqPathTreeNode, TSeqOperable>();

    process(operation: Iterable<TSeqRun>, remove = false) {
      for (let [runPath, content] of operation) {
        const { tree, anchor } = this.anchorAt(runPath);
        const parentPath = runPath.slice(0, -1);
        const [leafPid, leafIndex] = anchor.name;
        for (let i = 0; i < content.length; i++) {
          const charTick = content[i];
          const charIndex = leafIndex + i;
          const byIndex = tree[leafPid];
          const charNode = byIndex[charIndex] ??= { name: [leafPid, charIndex] };
          if (!remove) {
            const path = charNode === anchor ? runPath : [...parentPath, charNode.name];
            const operable = { path, charTick };
            Object.defineProperty(operable, 'next', {
              get: () => this.operables.get(byIndex[charIndex + 1])
            });
            // If there's redundancy between ops1 & ops2, this will remove it
            this.operables.set(charNode, operable);
          } else {
            // Remove operable ONLY IF it's strictly older
            const operable = this.operables.get(charNode);
            if (operable != null && !TSeqCharTick.inApplyOrder(charTick, operable.charTick))
              this.operables.delete(charNode);
          }
        }
      }
    }

    private anchorAt(runPath: TSeqName[]) {
      let node: TSeqPathTreeNode | null = null, tree = this.root;
      for (let name of runPath) {
        if (node != null) tree = (node.branch ??= {});
        const [pid, index] = name;
        node = (tree[pid] ??= {})[index] ??= { name };
      }
      return { tree, anchor: node! };
    }

    toRuns() {
      return toRuns(this.operables.values());
    }
  }

  /** Nodes will be used as map keys, so must be unique in the tree */
  type TSeqPathTreeNode = { name: TSeqName, branch?: TSeqOperationTree['root'] };

  /**
   * {@link apply Applying} operations requires unique paths; this utility
   * allows operation arrays to be concatenated safely, removing redundancy.
   */
  export function concat(...ops: TSeqOperation[]): TSeqOperation {
    const pathTree = new TSeqOperationTree();
    pathTree.process(concatIter(...ops));
    return [...pathTree.toRuns()];
  }

  /**
   * If an operation has been created by {@link concat concatenation}, and the
   * local replica has already applied part of the concatenated operation, this
   * utility safely removes the already-applied prefix.
   */
  export function rmPrefix(prefix: TSeqOperation, operation: TSeqOperation) {
    const pathTree = new TSeqOperationTree();
    pathTree.process(operation);
    pathTree.process(prefix, true);
    return [...pathTree.toRuns()];
  }

  /**
   * All nodes must still be attached to their containers, i.e. prior to garbage
   * collection.
   */
  export function toRevertOps(nodePrior: Map<TSeqCharNode, TSeqCharTick>): TSeqOperation {
    return [...TSeqOperable.toRuns(mapIter(nodePrior.keys(),
      function operable(node: TSeqCharNode): TSeqOperable {
        return {
          path: node.path,
          charTick: nodePrior.get(node)!,
          get next() {
            const nextNode = node.next;
            if (nextNode) return operable(nextNode);
          }
        };
      }
    ))];
  }
}