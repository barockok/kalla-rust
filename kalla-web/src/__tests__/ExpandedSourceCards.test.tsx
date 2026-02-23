import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { ExpandedSourceCards } from "@/components/wizard/steps/v2/ExpandedSourceCards";

global.fetch = jest.fn();

const props = {
  leftAlias: "invoices",
  rightAlias: "payments",
  leftLoaded: false,
  rightLoaded: false,
  onSourceLoaded: jest.fn(),
};

describe("ExpandedSourceCards", () => {
  beforeEach(() => jest.clearAllMocks());

  test("renders two source cards with headers", () => {
    render(<ExpandedSourceCards {...props} />);
    expect(screen.getByText("invoices")).toBeInTheDocument();
    expect(screen.getByText("payments")).toBeInTheDocument();
  });

  test("each card has Load from Source and Upload CSV tabs", () => {
    render(<ExpandedSourceCards {...props} />);
    const loadTabs = screen.getAllByText("Load from Source");
    const csvTabs = screen.getAllByText("Upload CSV");
    expect(loadTabs).toHaveLength(2);
    expect(csvTabs).toHaveLength(2);
  });

  test("clicking Load Sample triggers fetch to load-scoped", async () => {
    (global.fetch as jest.Mock).mockResolvedValue({
      ok: true,
      json: async () => ({
        alias: "invoices",
        columns: [{ name: "id", data_type: "integer", nullable: false }],
        rows: [["1"]],
        total_rows: 1,
        preview_rows: 1,
      }),
    });

    render(<ExpandedSourceCards {...props} />);
    const loadButtons = screen.getAllByRole("button", { name: /load sample/i });
    fireEvent.click(loadButtons[0]);

    await waitFor(() => {
      expect(global.fetch).toHaveBeenCalledWith(
        "/api/sources/invoices/load-scoped",
        expect.objectContaining({ method: "POST" }),
      );
    });
  });

  test("shows Loaded state with CheckCircle after successful load", async () => {
    (global.fetch as jest.Mock).mockResolvedValue({
      ok: true,
      json: async () => ({
        alias: "invoices",
        columns: [{ name: "id", data_type: "integer", nullable: false }],
        rows: [["1"]],
        total_rows: 1,
        preview_rows: 1,
      }),
    });

    render(<ExpandedSourceCards {...props} />);
    const loadButtons = screen.getAllByRole("button", { name: /load sample/i });
    fireEvent.click(loadButtons[0]);

    await waitFor(() => {
      expect(screen.getByText("Loaded")).toBeInTheDocument();
    });
  });

  test("calls onSourceLoaded callback after successful DB load", async () => {
    (global.fetch as jest.Mock).mockResolvedValue({
      ok: true,
      json: async () => ({
        alias: "invoices",
        columns: [{ name: "id", data_type: "integer", nullable: false }],
        rows: [["1"]],
        total_rows: 1,
        preview_rows: 1,
      }),
    });

    render(<ExpandedSourceCards {...props} />);
    const loadButtons = screen.getAllByRole("button", { name: /load sample/i });
    fireEvent.click(loadButtons[0]);

    await waitFor(() => {
      expect(props.onSourceLoaded).toHaveBeenCalledWith(
        "left",
        expect.objectContaining({ mode: "db", originalAlias: "invoices", loaded: true }),
        expect.objectContaining({ columns: expect.any(Array), rows: expect.any(Array) }),
      );
    });
  });

  test("switching to Upload CSV tab shows file upload UI", () => {
    render(<ExpandedSourceCards {...props} />);
    const csvTabs = screen.getAllByText("Upload CSV");
    fireEvent.click(csvTabs[0]);
    expect(screen.getByText(/drop a CSV file/i)).toBeInTheDocument();
  });

  test("shows already loaded indicator when leftLoaded is true", () => {
    render(<ExpandedSourceCards {...props} leftLoaded={true} />);
    // The left card should show the loaded state
    const loadedIndicators = screen.getAllByText("Loaded");
    expect(loadedIndicators.length).toBeGreaterThanOrEqual(1);
  });
});
