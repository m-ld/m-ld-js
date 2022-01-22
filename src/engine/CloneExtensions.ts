import {
  AppPrincipal, MeldConstraint, MeldExtensions, MeldTransportSecurity, noTransportSecurity
} from '../api';
import { Context } from '../jrql-support';
import { constraintFromConfig } from '../constraints';
import { DefaultList } from '../constraints/DefaultList';
import { InitialApp, MeldConfig } from '../config';

export class CloneExtensions implements MeldExtensions {
  private _transportSecurity: MeldTransportSecurity;

  static async initial(config: MeldConfig, app: InitialApp, context: Context) {
    let constraints = app.constraints ?? [];
    const transportSecurity = app.transportSecurity ?? noTransportSecurity;
    if (app.constraints == null)
      constraints = await Promise.all((config.constraints ?? [])
        .map(item => constraintFromConfig(item, context)));
    if (!constraints.find(constraint => constraint instanceof DefaultList))
      constraints.push(new DefaultList(config['@id']));
    return new CloneExtensions(constraints, transportSecurity, app.principal);
  }

  private constructor(
    public constraints: MeldConstraint[],
    transportSecurity: MeldTransportSecurity,
    private principal: AppPrincipal | undefined
  ) {
    // Use setter to ensure initialisation
    this.transportSecurity = transportSecurity;
  }

  get transportSecurity() {
    return this._transportSecurity;
  }

  set transportSecurity(transportSecurity: MeldTransportSecurity) {
    this._transportSecurity = transportSecurity;
    this._transportSecurity.setPrincipal?.(this.principal);
  }
}