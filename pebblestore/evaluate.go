package pebblestore

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/cockroachdb/pebble"

	"github.com/Arkiv-Network/pebble-bitmap-store/query"
	"github.com/Arkiv-Network/pebble-bitmap-store/store"
)

func evaluateAST(ctx context.Context, s *PebbleStore, reader pebble.Reader, ast *query.AST, block uint64) (*roaring64.Bitmap, error) {
	if ast.Expr == nil {
		if block == 0 {
			return s.EvaluateAllCurrent(ctx, reader)
		}
		return s.EvaluateAllAtBlock(ctx, reader, block)
	}
	return evaluateExpr(ctx, s, reader, &ast.Expr.Or, block)
}

func evaluateExpr(ctx context.Context, s *PebbleStore, reader pebble.Reader, or *query.ASTOr, block uint64) (*roaring64.Bitmap, error) {
	var result *roaring64.Bitmap

	for _, term := range or.Terms {
		bm, err := evaluateAnd(ctx, s, reader, &term, block)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = bm
		} else {
			result.Or(bm)
		}
	}

	return result, nil
}

func evaluateAnd(ctx context.Context, s *PebbleStore, reader pebble.Reader, and *query.ASTAnd, block uint64) (*roaring64.Bitmap, error) {
	var result *roaring64.Bitmap

	for _, term := range and.Terms {
		bm, err := evaluateTerm(ctx, s, reader, &term, block)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = bm
		} else {
			result.And(bm)
		}
	}

	return result, nil
}

func evaluateTerm(ctx context.Context, s *PebbleStore, reader pebble.Reader, term *query.ASTTerm, block uint64) (*roaring64.Bitmap, error) {
	switch {
	case term.Assign != nil:
		return evaluateEquality(ctx, s, reader, term.Assign, block)
	case term.Inclusion != nil:
		return evaluateInclusion(ctx, s, reader, term.Inclusion, block)
	case term.LessThan != nil:
		return evaluateLessThan(ctx, s, reader, term.LessThan, block)
	case term.LessOrEqualThan != nil:
		return evaluateLessOrEqualThan(ctx, s, reader, term.LessOrEqualThan, block)
	case term.GreaterThan != nil:
		return evaluateGreaterThan(ctx, s, reader, term.GreaterThan, block)
	case term.GreaterOrEqualThan != nil:
		return evaluateGreaterOrEqualThan(ctx, s, reader, term.GreaterOrEqualThan, block)
	case term.Glob != nil:
		return evaluateGlob(ctx, s, reader, term.Glob, block)
	default:
		return nil, fmt.Errorf("unknown term expression: %v", term)
	}
}

func evaluateEquality(ctx context.Context, s *PebbleStore, reader pebble.Reader, e *query.Equality, block uint64) (*roaring64.Bitmap, error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		if e.IsNot {
			values, err := s.GetMatchingStringValuesNotEqual(ctx, reader, e.Var, *e.Value.String, eb)
			if err != nil {
				return nil, err
			}
			return reconstructStringOR(ctx, s, reader, e.Var, values, block)
		}

		bm, err := s.ReconstructStringBitmapAtBlock(ctx, reader, e.Var, *e.Value.String, block)
		if err != nil {
			return nil, err
		}
		return bm.Bitmap, nil
	}

	if e.IsNot {
		values, err := s.GetMatchingNumericValuesNotEqual(ctx, reader, e.Var, *e.Value.Number, eb)
		if err != nil {
			return nil, err
		}
		return reconstructNumericOR(ctx, s, reader, e.Var, values, block)
	}

	bm, err := s.ReconstructNumericBitmapAtBlock(ctx, reader, e.Var, *e.Value.Number, block)
	if err != nil {
		return nil, err
	}
	return bm.Bitmap, nil
}

func evaluateInclusion(ctx context.Context, s *PebbleStore, reader pebble.Reader, e *query.Inclusion, block uint64) (*roaring64.Bitmap, error) {
	eb := store.EffectiveBlock(block)

	if len(e.Values.Strings) != 0 {
		var values []string
		var err error
		if e.IsNot {
			values, err = s.GetMatchingStringValuesNotInclusion(ctx, reader, e.Var, e.Values.Strings, eb)
		} else {
			values, err = s.GetMatchingStringValuesInclusion(ctx, reader, e.Var, e.Values.Strings, eb)
		}
		if err != nil {
			return nil, err
		}
		return reconstructStringOR(ctx, s, reader, e.Var, values, block)
	}

	var values []uint64
	var err error
	if e.IsNot {
		values, err = s.GetMatchingNumericValuesNotInclusion(ctx, reader, e.Var, e.Values.Numbers, eb)
	} else {
		values, err = s.GetMatchingNumericValuesInclusion(ctx, reader, e.Var, e.Values.Numbers, eb)
	}
	if err != nil {
		return nil, err
	}
	return reconstructNumericOR(ctx, s, reader, e.Var, values, block)
}

func evaluateLessThan(ctx context.Context, s *PebbleStore, reader pebble.Reader, e *query.LessThan, block uint64) (*roaring64.Bitmap, error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		values, err := s.GetMatchingStringValuesLessThan(ctx, reader, e.Var, *e.Value.String, eb)
		if err != nil {
			return nil, err
		}
		return reconstructStringOR(ctx, s, reader, e.Var, values, block)
	}

	values, err := s.GetMatchingNumericValuesLessThan(ctx, reader, e.Var, *e.Value.Number, eb)
	if err != nil {
		return nil, err
	}
	return reconstructNumericOR(ctx, s, reader, e.Var, values, block)
}

func evaluateLessOrEqualThan(ctx context.Context, s *PebbleStore, reader pebble.Reader, e *query.LessOrEqualThan, block uint64) (*roaring64.Bitmap, error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		values, err := s.GetMatchingStringValuesLessOrEqualThan(ctx, reader, e.Var, *e.Value.String, eb)
		if err != nil {
			return nil, err
		}
		return reconstructStringOR(ctx, s, reader, e.Var, values, block)
	}

	values, err := s.GetMatchingNumericValuesLessOrEqualThan(ctx, reader, e.Var, *e.Value.Number, eb)
	if err != nil {
		return nil, err
	}
	return reconstructNumericOR(ctx, s, reader, e.Var, values, block)
}

func evaluateGreaterThan(ctx context.Context, s *PebbleStore, reader pebble.Reader, e *query.GreaterThan, block uint64) (*roaring64.Bitmap, error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		values, err := s.GetMatchingStringValuesGreaterThan(ctx, reader, e.Var, *e.Value.String, eb)
		if err != nil {
			return nil, err
		}
		return reconstructStringOR(ctx, s, reader, e.Var, values, block)
	}

	values, err := s.GetMatchingNumericValuesGreaterThan(ctx, reader, e.Var, *e.Value.Number, eb)
	if err != nil {
		return nil, err
	}
	return reconstructNumericOR(ctx, s, reader, e.Var, values, block)
}

func evaluateGreaterOrEqualThan(ctx context.Context, s *PebbleStore, reader pebble.Reader, e *query.GreaterOrEqualThan, block uint64) (*roaring64.Bitmap, error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		values, err := s.GetMatchingStringValuesGreaterOrEqualThan(ctx, reader, e.Var, *e.Value.String, eb)
		if err != nil {
			return nil, err
		}
		return reconstructStringOR(ctx, s, reader, e.Var, values, block)
	}

	values, err := s.GetMatchingNumericValuesGreaterOrEqualThan(ctx, reader, e.Var, *e.Value.Number, eb)
	if err != nil {
		return nil, err
	}
	return reconstructNumericOR(ctx, s, reader, e.Var, values, block)
}

func evaluateGlob(ctx context.Context, s *PebbleStore, reader pebble.Reader, e *query.Glob, block uint64) (*roaring64.Bitmap, error) {
	eb := store.EffectiveBlock(block)

	var values []string
	var err error
	if e.IsNot {
		values, err = s.GetMatchingStringValuesNotGlob(ctx, reader, e.Var, e.Value, eb)
	} else {
		values, err = s.GetMatchingStringValuesGlob(ctx, reader, e.Var, e.Value, eb)
	}
	if err != nil {
		return nil, err
	}

	return reconstructStringOR(ctx, s, reader, e.Var, values, block)
}

// reconstructStringOR reconstructs a bitmap for each matching string value
// and ORs them together.
func reconstructStringOR(ctx context.Context, s *PebbleStore, reader pebble.Reader, name string, values []string, block uint64) (*roaring64.Bitmap, error) {
	bm := roaring64.New()
	for _, val := range values {
		reconstructed, err := s.ReconstructStringBitmapAtBlock(ctx, reader, name, val, block)
		if err != nil {
			return nil, err
		}
		if reconstructed != nil && reconstructed.Bitmap != nil {
			bm.Or(reconstructed.Bitmap)
		}
	}
	return bm, nil
}

// reconstructNumericOR reconstructs a bitmap for each matching numeric value
// and ORs them together.
func reconstructNumericOR(ctx context.Context, s *PebbleStore, reader pebble.Reader, name string, values []uint64, block uint64) (*roaring64.Bitmap, error) {
	bm := roaring64.New()
	for _, val := range values {
		reconstructed, err := s.ReconstructNumericBitmapAtBlock(ctx, reader, name, val, block)
		if err != nil {
			return nil, err
		}
		if reconstructed != nil && reconstructed.Bitmap != nil {
			bm.Or(reconstructed.Bitmap)
		}
	}
	return bm, nil
}
