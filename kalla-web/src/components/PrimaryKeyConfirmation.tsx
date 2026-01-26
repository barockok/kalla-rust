'use client';

import { useState, useEffect } from 'react';

interface PrimaryKeyConfirmationProps {
  sourceAlias: string;
  onConfirm: (keys: string[]) => void;
  onCancel: () => void;
}

interface PrimaryKeyResponse {
  alias: string;
  detected_keys: string[];
  confidence: string;
}

export function PrimaryKeyConfirmation({
  sourceAlias,
  onConfirm,
  onCancel,
}: PrimaryKeyConfirmationProps) {
  const [detectedKeys, setDetectedKeys] = useState<string[]>([]);
  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
  const [customKey, setCustomKey] = useState('');
  const [loading, setLoading] = useState(true);
  const [confidence, setConfidence] = useState('');

  useEffect(() => {
    async function fetchPrimaryKey() {
      try {
        const res = await fetch(`/api/sources/${sourceAlias}/primary-key`);
        if (res.ok) {
          const data: PrimaryKeyResponse = await res.json();
          setDetectedKeys(data.detected_keys);
          setSelectedKeys(data.detected_keys);
          setConfidence(data.confidence);
        }
      } catch (error) {
        console.error('Failed to detect primary key:', error);
      } finally {
        setLoading(false);
      }
    }
    fetchPrimaryKey();
  }, [sourceAlias]);

  const handleConfirm = () => {
    const keys = customKey ? [customKey] : selectedKeys;
    onConfirm(keys);
  };

  const toggleKey = (key: string) => {
    setSelectedKeys((prev) =>
      prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]
    );
    setCustomKey('');
  };

  if (loading) {
    return <div className="p-4">Detecting primary key for {sourceAlias}...</div>;
  }

  return (
    <div className="border rounded-lg p-4 bg-gray-50">
      <h3 className="font-semibold mb-2">
        Confirm Primary Key for &quot;{sourceAlias}&quot;
      </h3>

      {confidence === 'high' ? (
        <p className="text-sm text-gray-600 mb-3">
          Detected primary key with high confidence:
        </p>
      ) : (
        <p className="text-sm text-yellow-600 mb-3">
          Could not auto-detect primary key. Please specify:
        </p>
      )}

      <div className="space-y-2 mb-4">
        {detectedKeys.map((key) => (
          <label key={key} className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={selectedKeys.includes(key)}
              onChange={() => toggleKey(key)}
              className="rounded"
            />
            <span className="font-mono text-sm">{key}</span>
          </label>
        ))}
      </div>

      <div className="mb-4">
        <label className="block text-sm text-gray-600 mb-1">
          Or enter custom column name:
        </label>
        <input
          type="text"
          value={customKey}
          onChange={(e) => {
            setCustomKey(e.target.value);
            setSelectedKeys([]);
          }}
          placeholder="column_name"
          className="border rounded px-2 py-1 w-full font-mono text-sm"
        />
      </div>

      <div className="flex gap-2">
        <button
          onClick={handleConfirm}
          disabled={selectedKeys.length === 0 && !customKey}
          className="px-4 py-2 bg-blue-600 text-white rounded disabled:opacity-50"
        >
          Confirm
        </button>
        <button
          onClick={onCancel}
          className="px-4 py-2 border rounded"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}
