import { render, screen, act } from "@testing-library/react";
import { WizardProvider, useWizard } from "@/components/wizard/wizard-context";

function TestConsumer() {
  const { state, dispatch } = useWizard();
  return (
    <div>
      <span data-testid="step">{state.step}</span>
      <button onClick={() => dispatch({ type: "SET_STEP", step: 2 })}>Go to 2</button>
      <button
        onClick={() =>
          dispatch({
            type: "SET_SOURCES",
            left: { alias: "bank", uri: "pg://x", source_type: "postgres" },
            right: { alias: "inv", uri: "pg://y", source_type: "postgres" },
          })
        }
      >
        Set Sources
      </button>
      <span data-testid="left">{state.leftSource?.alias ?? "none"}</span>
    </div>
  );
}

describe("WizardContext", () => {
  it("provides initial state with step 1", () => {
    render(
      <WizardProvider><TestConsumer /></WizardProvider>,
    );
    expect(screen.getByTestId("step").textContent).toBe("1");
  });

  it("dispatches SET_STEP", async () => {
    render(
      <WizardProvider><TestConsumer /></WizardProvider>,
    );
    await act(async () => screen.getByText("Go to 2").click());
    expect(screen.getByTestId("step").textContent).toBe("2");
  });

  it("dispatches SET_SOURCES", async () => {
    render(
      <WizardProvider><TestConsumer /></WizardProvider>,
    );
    await act(async () => screen.getByText("Set Sources").click());
    expect(screen.getByTestId("left").textContent).toBe("bank");
  });
});
