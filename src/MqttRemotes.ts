import { Snapshot, DeltaMessage, MeldRemotes, Meld } from "./meld";
import { Observable } from 'rxjs';
import { TreeClock } from "./clocks";

export class MqttRemotes implements MeldRemotes {
  updates(): Observable<DeltaMessage> {
    throw new Error('Method not implemented.');
  }
  
  connect(clone: Meld): void {
    throw new Error("Method not implemented.");
  }
  
  newClock(): Promise<TreeClock> {
    throw new Error("Method not implemented.");
  }
  
  snapshot(): Promise<Snapshot> {
    throw new Error("Method not implemented.");
  }
  
  revupFrom(): Promise<Observable<DeltaMessage>> {
    throw new Error("Method not implemented.");
  }
}