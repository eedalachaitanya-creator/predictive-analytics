import { TestBed } from '@angular/core/testing';
import { App } from './app';

describe('App', () => {
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [App],
    }).compileComponents();
  });

  it('should create the app', () => {
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    expect(app).toBeTruthy();
  });

  // The default `ng new` "should render title" test (asserting a
  // 'Hello, walmart-analytics' welcome <h1>) was removed: App is the CRP router
  // shell and never had that template, so the scaffold assertion always failed.
});
