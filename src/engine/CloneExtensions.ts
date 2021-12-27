import {
  AppPrincipal, MeldConstraint, MeldExtensions, MeldTransportSecurity, noTransportSecurity
} from '../api';
import { Context } from '../jrql-support';
import { constraintFromConfig } from '../constraints';
import { DefaultList } from '../constraints/DefaultList';
import { MeldApp, MeldConfig } from '../config';

export class CloneExtensions implements MeldExtensions {
  private _transportSecurity: MeldTransportSecurity;
  private _constraints: MeldConstraint[];

  static async initial(config: MeldConfig, app: MeldApp, context: Context) {
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
    constraints: MeldConstraint[],
    transportSecurity: MeldTransportSecurity,
    private principal: AppPrincipal | undefined
  ) {
    // Use setters to ensure initialisation
    this.constraints = constraints;
    this.transportSecurity = transportSecurity;
  }

  get transportSecurity() {
    return this._transportSecurity;
  }

  set transportSecurity(transportSecurity: MeldTransportSecurity) {
    this._transportSecurity = transportSecurity;
    this._transportSecurity.setPrincipal?.(this.principal);
  }

  get constraints() {
    return this._constraints;
  }

  set constraints(constraints: MeldConstraint[]) {
    this._constraints = constraints;
  }
}