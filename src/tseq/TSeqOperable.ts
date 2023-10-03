import { mapIter } from '../engine/util';
import { TSeqCharTick, TSeqLocalOperation, TSeqName, TSeqOperation, TSeqRevert } from './types';

/**
 * Utility interface for manipulating {@link TSeqOperation TSeqOperations}. This
 * takes advantage of the observation that some functions over character nodes
 * in a TSeq can also be applied to {@link TSeqRun runs in an operation}.
 * @internal
 */
export interface TSeqOperable {
  path: TSeqName[],
  charTick: TSeqCharTick,
  pre: TSeqCharTick | undefined,
  /** Contiguous next – if undefined, nothing contiguous in context */
  next?: TSeqOperable
}

/**
 * Static functions applicable to {@link TSeqOperable} objects.
 * @internal
 */
export namespace TSeqOperable {
  /**
   * Processes the given operables into a set of "runs", which comprise an
   * anchor path and contiguous char-ticks following it
   */
  export function toRuns(nodes: Iterable<TSeqOperable>, revert?: TSeqRevert): TSeqOperation {
    const anchors = new Map(mapIter(nodes, node => [node, [] as TSeqOperable[]]));
    for (let [node, run] of anchors) {
      for (let next = node.next; next != null && anchors.has(next); next = next.next) {
        run.push(next, ...anchors.get(next)!);
        anchors.delete(next);
      }
    }
    return [...anchors].map(([node, run], r) => {
      run.unshift(node);
      return [node.path, run.map((runNode, i) => {
        if (runNode.pre && revert != null)
          (revert[r] ??= [])[i] = runNode.pre;
        return runNode.charTick;
      })];
    });
  }

  class TSeqOperationTree {
    root: { [pid: string]: { [index: number]: TSeqPathTreeNode } } = {};
    operables = new Map<TSeqPathTreeNode, TSeqOperable>();

    process(operation: TSeqOperation, remove?: TSeqRevert | true) {
      operation.forEach(([runPath, run], r) => {
        const { tree, anchor } = this.anchorAt(runPath);
        const parentPath = runPath.slice(0, -1);
        const [leafPid, leafIndex] = anchor.name;
        run.forEach((charTick, i) => {
          const charIndex = leafIndex + i;
          const byIndex = tree[leafPid];
          const charNode = byIndex[charIndex] ??= { name: [leafPid, charIndex] };
          if (remove !== true) {
            const path = charNode === anchor ? runPath : [...parentPath, charNode.name];
            // Take the earliest available pre-image of the node
            const pre = this.operables.get(charNode)?.pre ?? (remove && remove[r][i]);
            const operable = { path, charTick, pre };
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
        });
      });
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

    toOperation(revert?: TSeqRevert): TSeqOperation {
      return toRuns(this.operables.values(), revert);
    }
  }

  /** Nodes will be used as map keys, so must be unique in the tree */
  type TSeqPathTreeNode = { name: TSeqName, branch?: TSeqOperationTree['root'] };

  /**
   * {@link apply Applying} operations requires unique paths; this utility
   * allows operation arrays to be concatenated safely, removing redundancy.
   */
  export function concat(...ops: TSeqLocalOperation[]): TSeqLocalOperation {
    const pathTree = new TSeqOperationTree();
    for (let [operation, revert] of ops)
      pathTree.process(operation, revert);
    const revert: TSeqRevert = [];
    return [pathTree.toOperation(revert), revert];
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
    return pathTree.toOperation();
  }
}